package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

object IpSpamDetectorSparkApproxBatches {

  // Takes (Long, A) pairs and sorts them by the first member of
  // the tuple (the timestamp)
  class TimestampOrdering[A] extends Ordering[(Long, A)] {
    def compare(x: (Long, A), y: (Long, A)): Int = x._1.compare(y._1)
  }

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Testing")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
//  conf.set("spark.kryo.registrationRequired", "true")
//  conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.TreeSet," +
//    "scala.collection.mutable.WrappedArray")

  def main(args: Array[String]) {

    if (args.length < 4 || args.length > 5) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar master-ip baseFilename numPartitions numBatches [ printDebug=false ]
          |  Example values:
          |    master-ip      = 171.31.31.42
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    numBatches     = 100
          |    printDebug     = true
        """.stripMargin)
      System.exit(1)
    }

    if (args(0) == "local") {
      conf.setMaster(s"local[${args(2)}]")
    } else {
      conf.setMaster(s"spark://${args(0)}:7077")
    }
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    val baseFn = args(1)
    val numPartitions = args(2).toInt
    val numBatches = args(3).toInt
    val printDebug = if (args.length > 4 && args(4) == "true") true else false

    var batchNum = 0

    var messageCount = 0L
    var newFriendEventCount = 0L

    var spamCounts = 0L
    var allFriendships: RDD[((Long, Long), Int)] = sc.emptyRDD[((Long, Long), Int)]
    var userFriendMessageCounts: RDD[(Long, (Int, Int))] = sc.emptyRDD[(Long, (Int, Int))]
    var ipMessageCounts: RDD[(Int, (Int, Int))] = sc.emptyRDD[(Int, (Int, Int))]
//    var spamCountByUser: RDD[(Long, Int)] = sc.emptyRDD[(Long, Int)]

    for (i <- 0 until numBatches) {
      val batchFn = s"$baseFn-$batchNum"
      val filenames = (0 until numPartitions).map(i => s"$batchFn-$i").toArray
      val events = KryoLoad.loadFiles(sc, filenames)
      val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
      val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

      messageCount += messageEvents.count()
      newFriendEventCount += newFriendEvents.count()

      val spamMessagesFromMessageSendCt = messageEvents.map(me => me.senderUserId -> me.messageId)
        .leftOuterJoin(userFriendMessageCounts)
        .filter({
          case (sendId, (_, Some((f, nf)))) => f + nf > 5 && nf > 2 * f
          case (sendId, (_, None)) => false
        })
        .map({case (sendId, (mId, _)) => mId})
//        .aggregateByKey(0)((cnt, n) => cnt + 1, (cnt1, cnt2) => cnt1 + cnt2)

      val spamMessagesFromIpSendCt = messageEvents.map(me => me.senderIp -> me.messageId)
        .leftOuterJoin(ipMessageCounts)
        .filter({
          case (sendIp, (_, Some((email, noEmail)))) =>  email.toDouble / (email + noEmail).toDouble > 0.2
          case (sendIp, (_, None)) => false
        })
        .map({case (sendIp, (mId, _)) => mId})

      spamCounts += spamMessagesFromIpSendCt.intersection(spamMessagesFromMessageSendCt).count()

//      spamCountByUser = spamCountByUser.fullOuterJoin(newSpamCounts)
//        .mapValues({
//          case (Some(a), Some(b)) => a + b
//          case (Some(a), None)    => a
//          case (None, Some(b))    => b
//          case _ => 0
//        })
        //.foldByKey(0)(_ + _)

      val messagesSentToFriend = messageEvents.map(me => ((me.senderUserId, me.recipientUserId), 1))
        .foldByKey(0)(_ + _)
        .leftOuterJoin(allFriendships)
        .map({
          case ((sendId, rcvId), (cnt, Some(friend))) => sendId -> (cnt, 0) // friends
          case ((sendId, rcvId), (cnt, None)) =>         sendId -> (0, cnt) // not friends
        })
        .foldByKey((0, 0))(addPairs[Int])

      allFriendships = allFriendships.fullOuterJoin(newFriendEvents.flatMap(nfe =>
        List((nfe.userIdA, nfe.userIdB) -> 1, (nfe.userIdB, nfe.userIdA) -> 1)
      )).mapValues({
        case (Some(a), Some(b)) => a + b
        case (Some(a), None)    => a
        case (None, Some(b))    => b
        case _ => 0
      })
      //.foldByKey(0)(_ + _)

      userFriendMessageCounts = userFriendMessageCounts.fullOuterJoin(messagesSentToFriend).mapValues(addPairsOption[Int])

      val ipSendCt = messageEvents.map(me => me.senderIp -> hasEmail(me.content))
        .aggregateByKey((0, 0))({
          case ((email, noEmail), hasEmail) =>
            if (hasEmail) { (email + 1, noEmail) } else { (email, noEmail + 1) }
        }, addPairs[Int])

      ipMessageCounts = ipMessageCounts.fullOuterJoin(ipSendCt).mapValues(addPairsOption[Int])

      batchNum += 1
    }

    if (printDebug) {
//      println(s"Number of users: ${spamCountByUser.count()}")
//      println(s"Top 20 spammers: ${spamCountByUser.takeOrdered(20)(new Ordering[(Long, Int)] {
//        def compare(x: (Long, Int), y: (Long, Int)) = -1 * x._2.compare(y._2)
//      }).mkString(", ")}")
    }

    //println(s"FINAL SPAM COUNT: ${spamCountByUser.map({case (id, cnt) => cnt}).sum}")
    println(s"FINAL SPAM COUNT: $spamCounts")

    val endTime = System.currentTimeMillis() - startTime
    println(s"Final runtime was $endTime ms (${endTime / 1000} sec)")
    println(s"Process rate was ${(newFriendEventCount + messageCount) / (endTime / 1000)} per second")
  }
}

