package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SimpleSpamDetectorSparkApproxBatches {

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
          |    master-ip      = 171.41.41.31
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

    var allFriendships: RDD[((Long, Long), Int)] = sc.emptyRDD[((Long, Long), Int)]
    var userFriendMessageCounts: RDD[(Long, (Int, Int))] = sc.emptyRDD[(Long, (Int, Int))]
    var spamCountByUser: RDD[(Long, Int)] = sc.emptyRDD[(Long, Int)]

    var messageEventCount = 0L
    var newFriendEventCount = 0L

    for (batchNum <- 0 until numBatches) {
      val batchFn = s"$baseFn-$batchNum"
      val filenames = (0 until numPartitions).map(i => s"$batchFn-$i")
      val events = KryoLoad.loadFiles(sc, filenames)
      val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
      val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

      messageEventCount += messageEvents.count()
      newFriendEventCount += newFriendEvents.count()

      val newSpamCounts = messageEvents.map(_.senderUserId -> 1)
        .leftOuterJoin(userFriendMessageCounts)
        .filter({
          case (sendId, (_, Some((f, nf)))) => f + nf > 5 && nf > 2 * f
          case (sendId, (_, None)) => false
        })
        .aggregateByKey(0)((cnt, n) => cnt + 1, (cnt1, cnt2) => cnt1 + cnt2)

      spamCountByUser = spamCountByUser.fullOuterJoin(newSpamCounts)
        .mapValues({
          case (Some(a), Some(b)) => a + b
          case (Some(a), None)    => a
          case (None, Some(b))    => b
          case _ => 0
        })
        //.foldByKey(0)(_ + _)

      val messagesSentToFriend = messageEvents.map(me => ((me.senderUserId, me.recipientUserId), 1))
        .foldByKey(0)(_ + _)
        .leftOuterJoin(allFriendships)
        .map({
          case ((sendId, rcvId), (cnt, Some(friend))) => sendId -> (cnt, 0) // friends
          case ((sendId, rcvId), (cnt, None)) =>         sendId -> (0, cnt) // not friends
        })
        .foldByKey((0, 0))({case ((f1, nf1), (f2, nf2)) => (f1 + f2, nf1 + nf2)})

      allFriendships = allFriendships.fullOuterJoin(newFriendEvents.flatMap(nfe =>
        List((nfe.userIdA, nfe.userIdB) -> 1, (nfe.userIdB, nfe.userIdA) -> 1)
      )).mapValues({
        case (Some(a), Some(b)) => a + b
        case (Some(a), None)    => a
        case (None, Some(b))    => b
        case _ => 0
      })
      //.foldByKey(0)(_ + _)

      userFriendMessageCounts = userFriendMessageCounts
        .fullOuterJoin(messagesSentToFriend)
        .mapValues({
          case (Some((f1, nf1)), Some((f2, nf2))) => (f1 + f2, nf1 + nf2)
          case (Some((f1, nf1)), None)            => (f1, nf1)
          case (None, Some((f2, nf2)))            => (f2, nf2)
          case _ => (0, 0)
        })
        //.foldByKey((0, 0))({case ((f1, nf1), (f2, nf2)) => (f1 + f2, nf1 + nf2)})
    }

    if (printDebug) {
      println(s"Number of users: ${spamCountByUser.count()}")
      println(s"Top 20 spammers: ${spamCountByUser.takeOrdered(20)(new Ordering[(Long, Int)] {
        def compare(x: (Long, Int), y: (Long, Int)) = -1 * x._2.compare(y._2)
      }).mkString(", ")}")
    }

    println(s"FINAL SPAM COUNT: ${spamCountByUser.map({case (id, cnt) => cnt}).sum}")

    val endTime = System.currentTimeMillis() - startTime
    println(s"Final runtime was $endTime ms (${endTime / 1000} sec)")
    println(s"Process rate was ${(newFriendEventCount + messageEventCount) / (endTime / 1000)} per second")
  }

}
