package replaydb

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import replaydb.event.{MessageEvent, NewFriendshipEvent}

object IpSpamDetectorSparkStreaming {

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Streaming Testing")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
  conf.set("spark.streaming.backpressure.enabled", "true")
//  conf.set("spark.streaming.receiver.maxRate", "1000")
//  conf.set("spark.kryo.registrationRequired", "true")
//  conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.TreeSet," +
//    "scala.collection.mutable.WrappedArray")

  def main(args: Array[String]) {

    if (args.length != 7) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar master-ip awsAccessKey awsSecretKey baseFilename numPartitions batchSizeMs totalEvents
          |  Example values:
          |    master-ip      = 171.41.41.31
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    batchSizeMs    = 1000
          |    totalEvents    = 500000
        """.stripMargin)
      System.exit(1)
    }

    if (args(0) == "local") {
      conf.setMaster(s"local[${args(2)+1}]")
    } else {
      conf.setMaster(s"spark://${args(0)}:7077")
    }

    val baseFn = args(3)
    val numPartitions = args(4).toInt
    val batchSizeMs = args(5).toInt
    val totalEvents = args(6).toInt

    val ssc = new StreamingContext(conf, Milliseconds(batchSizeMs))
    val hadoopConf = ssc.sparkContext.hadoopConfiguration;
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3.awsAccessKeyId", args(1))
    hadoopConf.set("fs.s3.awsSecretAccessKey", args(2))

    ssc.checkpoint("s3://spark-restream/checkpoint")

    val kryoReceivers = (0 until numPartitions).map(i => new KryoFileReceiver(s"$baseFn-$i"))
    val kryoStreams = kryoReceivers.map(ssc.receiverStream(_))
    val eventStream = ssc.union(kryoStreams)

    val messageEvents = eventStream.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
    val newFriendEvents = eventStream.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

    val msgsSent = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> me.messageId).groupByKey().mapValues(_.toList)
    val friends = newFriendEvents.flatMap(nfe => List((nfe.userIdA, nfe.userIdB) -> 1, (nfe.userIdB, nfe.userIdA) -> 1))

    val msgsSentWithFriends = msgsSent.fullOuterJoin(friends)

    // Returns userId -> (toFriendCount, List(all sent messageIDs))
    def friendStateUpdateFunction(key: LongPair, value: Option[(Option[List[Long]], Option[Int])],
                                  state: State[Boolean]): (Long, (Int, List[Long])) = value match {
      case None => ???
      case Some((sentMessagesListOpt, friendOpt)) =>
        val sentMessages = sentMessagesListOpt.getOrElse(List())
        val friends = state.getOption().getOrElse(false)
        if (friendOpt.nonEmpty) {
          state.update(friendOpt.get > 0)
        }
        key._1 -> (if (friends) sentMessages.size else 0, sentMessages)
    }
    val friendSpec = StateSpec.function(friendStateUpdateFunction _).numPartitions(numPartitions)
    val userNewSendCt = msgsSentWithFriends.mapWithState[Boolean, (Long, (Int, List[Long]))](friendSpec)
      .reduceByKey((a, b) => (a._1+b._1, a._2 ::: b._2))

    def userSendCtUpdateFunction(key: Long, value: Option[(Int, List[Long])], state: State[IntPair]): List[Long] = value match {
      case None => ???
      case Some((friendSendCt, messageIds)) =>
        val oldCt = state.getOption().getOrElse((0, 0))
        val isSpam = oldCt._1 + oldCt._2 > 5 && oldCt._2 > 2 * oldCt._1
        state.update(addPairs(oldCt, (friendSendCt, messageIds.size - friendSendCt)))
        if (isSpam) messageIds else List()
    }
    val userCountSpec = StateSpec.function(userSendCtUpdateFunction _).numPartitions(numPartitions)
    val userNewSpamMessages = userNewSendCt.mapWithState[IntPair, List[Long]](userCountSpec).flatMap(_.map(_ -> 0))

    val msgsByIp = messageEvents.map(me => me.senderIp -> (hasEmail(me.content), me.messageId))
    def ipSendCtUpdateFunction(key: Int, value: Option[(Boolean, Long)], state: State[IntPair]): Option[Long] = value match {
      case None => ???
      case Some((hasEmail, messageId)) =>
        val oldCt = state.getOption().getOrElse((0, 0))
        val isIPSpam = oldCt._1 + oldCt._2 > 0 && (oldCt._1.toDouble / (oldCt._1 + oldCt._2).toDouble > 0.2)
        state.update(addPairs(oldCt, if (hasEmail) (1, 0) else (0, 1)))
        if (isIPSpam) Some(messageId) else None
    }
    val ipSpec = StateSpec.function(ipSendCtUpdateFunction _).numPartitions(numPartitions)
    val ipNewSpamMessages = msgsByIp.mapWithState[IntPair, Option[Long]](ipSpec).filter(_.nonEmpty).map(_.get ->0)

    val spamMessageIDs = userNewSpamMessages.join(ipNewSpamMessages)

    var totalSpam = 0.0
    spamMessageIDs.foreachRDD(rdd => {
      val spamCount = rdd.count
      totalSpam += spamCount
      if (spamCount > 0) println(s"Count of new spam: $spamCount; total is $totalSpam")
    })

    val eventsProcessed = new AtomicLong
    eventStream.foreachRDD(rdd => {
      val count = rdd.count()
      eventsProcessed.getAndAdd(count)
      //      if (count > 0) println(s"Events processed in this batch: $count; total is $totalEvents")
    })

    ssc.start()
    val startTime = System.currentTimeMillis()
    new Thread("Closer") {
      override def run(): Unit = {
        while (eventsProcessed.get() < totalEvents) {
          Thread.sleep(1000)
        }
        val processTime = System.currentTimeMillis() - startTime
        println(s"IpSpamDetectorSparkStreaming: $numPartitions partitions, $totalEvents events in $processTime ms (${totalEvents/(processTime/1000)} events/sec)")
        println(s"CSV,IpSpamDetectorSparkStreaming,$numPartitions,$totalEvents,$processTime,$batchSizeMs,$totalSpam")
        ssc.stop(true)
      }
    }.start()
    ssc.awaitTermination()
//
//    if (printDebug) {
//      println(s"Number of users: ${userSpamCount.count()}")
////      println(s"Top 20 spammers: ${userSpamCount.takeOrdered(20)(new Ordering[(Long, Int)] {
////        def compare(x: (Long, Int), y: (Long, Int)) = -1 * x._2.compare(y._2)
////      }).mkString(", ")}")
//    }
//
//    println(s"FINAL SPAM COUNT: ${userSpamCount.map({case (id, cnt) => cnt}).compute(Time(System.currentTimeMillis())).get.sum}")

    val endTime = System.currentTimeMillis() - startTime
    println(s"Final runtime was $endTime ms (${endTime / 1000} sec)")
//    println(s"Process rate was ${(newFriendEventCount + messageEventCount) / (endTime / 1000)} per second")
  }

}
