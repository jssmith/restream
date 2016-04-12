package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SimpleSpamDetectorSpark {

  // Takes (Long, A) pairs and sorts them by the first member of
  // the tuple (the timestamp)
  class TimestampOrdering[A] extends Ordering[(Long, A)] {
    def compare(x: (Long, A), y: (Long, A)): Int = x._1.compare(y._1)
  }

  class QueueTimestampOrdering extends Ordering[Queue[(Long, (Int, Int))]] {
    def compare(x: Queue[(Long, (Int, Int))], y: Queue[(Long, (Int, Int))]): Int = {
      -1 * x.head._1.compare(y.head._1)
    }
  }

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Testing")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
//  conf.set("spark.kryo.registrationRequired", "true")
//  conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.TreeSet," +
//    "scala.collection.mutable.WrappedArray")

  def main(args: Array[String]) {

    if (args.length < 3 || args.length > 4) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar master-ip baseFilename numPartitions [ printDebug=false ]
          |  Example values:
          |    master-ip      = 172.31.31.41
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    printDebug     = true
        """.stripMargin)
      System.exit(1)
    }

    conf.setMaster(s"spark://${args(0)}:7077")
    val sc = new SparkContext(conf)

    val startTime = System.currentTimeMillis()

    val baseFn = args(1)
    val numPartitions = args(2).toInt
    val printDebug = if (args.length > 3 && args(3) == "true") true else false
    val filenames = (0 until numPartitions).map(i => s"$baseFn-$i").toArray

    val events = KryoLoad.loadFiles(sc, filenames)

    val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
    val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

    val messageCount = messageEvents.count()
    val newFriendEventCount = newFriendEvents.count()
    if (printDebug) {
      println(s"Message count $messageCount // newFriendEvent count $newFriendEventCount")
      println(s"Number of distinct users is ${messageEvents.map(_.senderUserId).distinct().count()}")
    }

    // for now just keep both friendship twice for simplicity sake
    val friendships = newFriendEvents.flatMap((nfe: NewFriendshipEvent) => {
      List((nfe.userIdA, nfe.userIdB) -> List((nfe.ts, 1)),
        (nfe.userIdB, nfe.userIdA) -> List((nfe.ts, 1)))
    }) // mapping (id, id) -> List[(ts, val)] pairs

    if (printDebug) println(s"Size of friendships RDD: ${friendships.count()}")

    // decide, on each message send, whether it was sent to a friend or not
    val msgsWithTs = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> me.ts)
    if (printDebug) println(s"Size of msgsWithTs RDD: ${msgsWithTs.count()}")


    // Combine together messages from the same user to cut down on the amount of joining done
    val msgsWithTsAgg = msgsWithTs.aggregateByKey(ArrayBuffer[Long]())((buf, v) => {
      buf.insert(buf.lastIndexWhere(_ < v) + 1, v)
      buf
    }, (buf1, buf2) => {
      var idx1 = 0
      var idx2 = 0
      val outBuf = new ArrayBuffer[Long](buf1.length + buf2.length)
      while (idx1 < buf1.length || idx2 < buf2.length) {
        if (idx1 == buf1.length) {
          outBuf.append(buf2(idx2))
          idx2 += 1
        } else if ((idx2 == buf2.length) || (buf1(idx1) < buf2(idx2))) {
          outBuf.append(buf1(idx1))
          idx1 += 1
        } else {
          outBuf.append(buf2(idx2))
          idx2 += 1
        }
      }
      outBuf
    })

    if (printDebug) println(s"Size of msgsWithTsAgg: ${msgsWithTsAgg.count()}, " +
      s"message count is ${msgsWithTsAgg.map(_._2).map(_.size).sum()}")

     val usersWithMsgSentToFriendOrNot = msgsWithTsAgg.leftOuterJoin(friendships).map({
      case ((idA, idB), (sortedTs, Some(list))) =>
        var sortedList = Queue[(Long, (Int, Int))]()
        val iteratorTs = sortedTs.toIterator
        var currentListIndex = 0
        var currentFriendshipValue = 0
        while (iteratorTs.hasNext) {
          val ts = iteratorTs.next()
          while (currentListIndex < list.size && ts > list(currentListIndex)._1) {
            currentFriendshipValue = list(currentFriendshipValue)._2
            currentListIndex += 1
          }
          sortedList = sortedList :+ ts -> (if (currentFriendshipValue > 0) {
            (1, 0) // Friends
          } else {
            (0, 1) // Not friends
          })
        }
        idA -> sortedList
      case ((idA, idB), (sortedTs, None)) => // At no point were they ever friends
        var sortedList = Queue[(Long, (Int, Int))]()
        for (ts <- sortedTs) {
          sortedList = sortedList :+ ts -> (0, 1)
        }
        idA -> sortedList
    })

    if (printDebug) println(s"Size of usersWithMsgSentToFriendOrNot is ${usersWithMsgSentToFriendOrNot.count()}, " +
      s"number of messages is ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_ => true)).map(_._2).sum()} " +
      s"with ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_._2._1 == 1)).map(_._2).sum()} sent to friends " +
      s"and ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_._2._2 == 1)).map(_._2).sum()} to nonfriends")

    // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
    val usersWithMsgSendCts = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
      var outList = Queue[(Long, (Int, Int))]()
      val queues = mutable.PriorityQueue()(new QueueTimestampOrdering)
      queues ++= listOfQueues
      var runningTotal = (0, 0)
      while (queues.nonEmpty) {
        val nextQueue = queues.dequeue()
        val (next, q) = nextQueue.dequeue
        if (q.nonEmpty) {
          queues.enqueue(q)
        }
        runningTotal = (runningTotal._1 + next._2._1, runningTotal._2 + next._2._2)
        outList = outList :+ next._1 -> runningTotal
      }
      outList
    })

    if (printDebug) println(s"Size of usersWithMsgSendCts is ${usersWithMsgSendCts.count()}, " +
      s"Number of messages: ${usersWithMsgSendCts.mapValues(_.count(_ => true)).map(_._2).sum()}")

    val spamCtByUser = usersWithMsgSendCts.mapValues(_.count({
      case (_, (friendCt, nonfriendCt)) =>
        friendCt + nonfriendCt > 5 && nonfriendCt > 2 * friendCt
    }))
    if (printDebug) {
      println(s"Number of users: ${spamCtByUser.count()}")
      println(s"Top 20 spammers: ${spamCtByUser.takeOrdered(20)(new Ordering[(Long, Int)] {
        def compare(x: (Long, Int), y: (Long, Int)) = -1 * x._2.compare(y._2)
      }).mkString(", ")}")
    }

    val spamCount = spamCtByUser.map(_._2).sum()

    println(s"Final spam count is: $spamCount from ${spamCtByUser.filter(_._2 > 0).count()} users")

    val endTime = System.currentTimeMillis() - startTime
    println(s"Final runtime was $endTime ms (${endTime / 1000} sec)")
    println(s"Process rate was ${(newFriendEventCount + messageCount) / (endTime / 1000)} per second")
  }
}

