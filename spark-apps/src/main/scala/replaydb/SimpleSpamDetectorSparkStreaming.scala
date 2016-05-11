package replaydb

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import replaydb.event.{MessageEvent, NewFriendshipEvent}

object SimpleSpamDetectorSparkStreaming {

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Streaming Testing")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
  conf.set("spark.streaming.backpressure.enabled", "true")
//  conf.set("spark.streaming.receiver.maxRate", "1000")
//  conf.set("spark.kryo.registrationRequired", "true")
//  conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.TreeSet," +
//    "scala.collection.mutable.WrappedArray")

  def main(args: Array[String]) {

    if (args.length != 4) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar master-ip baseFilename numPartitions batchSizeMs
          |  Example values:
          |    master-ip      = 171.41.41.31
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    batchSizeMs    = 1000
        """.stripMargin)
      System.exit(1)
    }

    if (args(0) == "local") {
      conf.setMaster(s"local[${args(2)+1}]")
    } else {
      conf.setMaster(s"spark://${args(0)}:7077")
    }

    val baseFn = args(1)
    val numPartitions = args(2).toInt
    val batchSizeMs = args(3).toInt

    val ssc = new StreamingContext(conf, Milliseconds(batchSizeMs))
    ssc.checkpoint("/tmp/spark_streaming_checkpoint")

    val startTime = System.currentTimeMillis()

    val kryoReceivers = (0 until numPartitions).map(i => new KryoFileReceiver(s"$baseFn-$i"))
    val kryoStreams = kryoReceivers.map(ssc.receiverStream(_))
    val eventStream = ssc.union(kryoStreams)

    val messageEvents = eventStream.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
    val newFriendEvents = eventStream.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

    val msgsSent = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> 1).groupByKey().mapValues(msgs => msgs.size)
    val friends = newFriendEvents.flatMap(nfe => List((nfe.userIdA, nfe.userIdB) -> 1, (nfe.userIdB, nfe.userIdA) -> 1))

    val msgsSentWithFriends = msgsSent.fullOuterJoin(friends)

    // Returns (messagesToFriends, messagesToNonfriends) in this interval for the given (sender, rcvr) pair
    def friendStateUpdateFunction(key: LongPair, value: Option[(Option[Int], Option[Int])], state: State[Boolean]): (Long, IntPair) = value match {
      case None => ???
      case Some((sendCtOpt, friendOpt)) =>
        val sendCt = sendCtOpt.getOrElse(0)
        val friends = state.getOption().getOrElse(false)
        if (friendOpt.nonEmpty) {
          state.update(friendOpt.get > 0)
        }
        key._1 -> (if (friends) (sendCt, 0) else (0, sendCt))
    }
    val friendSpec = StateSpec.function(friendStateUpdateFunction _).numPartitions(numPartitions)
    val userNewSendCt = msgsSentWithFriends.mapWithState[Boolean, (Long, IntPair)](friendSpec).reduceByKey(addPairs[Int])

    def userSendCtUpdateFunction(key: Long, value: Option[IntPair], state: State[IntPair]): (Long, Int) = value match {
      case None => ???
      case Some(newCt) =>
        val oldCt = state.getOption().getOrElse((0, 0))
        val isSpam = oldCt._1 + oldCt._2 > 5 && oldCt._2 > 2 * oldCt._1
        val spamCt = if (isSpam) newCt._1+newCt._2 else 0
        state.update(addPairs(oldCt, newCt))
        key -> spamCt
    }
    val userCountSpec = StateSpec.function(userSendCtUpdateFunction _).numPartitions(numPartitions)
    val userNewSpamCount = userNewSendCt.mapWithState[IntPair, (Long, Int)](userCountSpec)

    var totalSpam = 0.0
    userNewSpamCount.foreachRDD(rdd => {
      val spamCount = rdd.map(_._2).sum
      totalSpam += spamCount
      if (spamCount > 0) println(s"Count of new spam: $spamCount; total is $totalSpam")
    })

    var totalEvents = 0.0
    eventStream.foreachRDD(rdd => {
      val count = rdd.count()
      totalEvents += count
      if (count > 0) println(s"Events processed in this batch: $count; total is $totalEvents")
    })

    ssc.start()
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
