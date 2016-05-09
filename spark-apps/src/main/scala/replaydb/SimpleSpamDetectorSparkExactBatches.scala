package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SimpleSpamDetectorSparkExactBatches {

  // Takes (Long, A) pairs and sorts them by the first member of
  // the tuple (the timestamp)
  class TimestampOrdering[A] extends Ordering[TSVal[A]] {
    def compare(x: (Long, A), y: (Long, A)): Int = x._1.compare(y._1)
  }

  class BufferTimestampOrdering[A] extends Ordering[ListBuffer[TSVal[A]]] {
    def compare(x: ListBuffer[TSVal[A]], y: ListBuffer[TSVal[A]]): Int = {
      -1 * x.head._1.compare(y.head._1)
    }
  }
  //  type TSValList[A] = List[TSVal[A]]
  type TSVal[A] = (Long, A)

  class TSValList[A](items: TSVal[A]*) {
    val list: ListBuffer[TSVal[A]] = new ListBuffer[TSVal[A]]()
    list ++= items

    // Combine takes a total and an incremental and returns a new total
    // Assumes that the lists are currently in sorted order (they should never not be)
    def merge(incremental: TSValList[A], combine: (A, A) => A): TSValList[A] = {
      if (list.last._1 > incremental.list.head._1) {
        throw new IllegalArgumentException("All of the incremental list TS must be after the current list!")
      }
      var runningTotal = list.last._2
      for (inc <- incremental.list) {
        runningTotal = combine(runningTotal, inc._2)
        list.append(inc._1 -> runningTotal)
      }
      this
    }

    // Consider *this* to be an incremental list. Start with zeroVal, and combine
    // one-at-a-time the value into the running total. Length of return is same as
    // length of input
    def sumOverIncremental[B](zeroVal: B, combine: (B, A) => B): TSValList[B] = {
      var runningTotal = zeroVal
      val newList = new TSValList[B]()
      for ((ts, value) <- list) {
        runningTotal = combine(runningTotal, value)
        newList.list.append(ts -> runningTotal)
      }
      newList
    }

    //
    def evaluateAgainst[B, C](state: Option[TSValList[B]], evaluate: (A, Option[B]) => C): TSValList[C] = {
      var currentState: Option[B] = None
      val newList = new TSValList[C]()
      var nextStates = if (state.isEmpty) ListBuffer() else state.get.list
      for ((ts, value) <- list) {
        if (nextStates.nonEmpty && nextStates.head._1 <= ts) {
          // next state is applicable to us
          currentState = Some(nextStates.head._2)
          nextStates = nextStates.tail
        }
        newList.list.append(ts -> evaluate(value, currentState))
      }
      newList
    }
  }

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Testing").setMaster("local[4]")
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
          |          |    master-ip      = 171.41.41.31
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

    var allFriendships: RDD[((Long, Long), TSValList[Int])] = sc.emptyRDD[((Long, Long), TSValList[Int])]
    var userFriendMessageCounts: RDD[(Long, TSValList[(Int, Int)])] = sc.emptyRDD[(Long, TSValList[(Int, Int)])]
    var spamCountByUser: RDD[(Long, Int)] = sc.emptyRDD[(Long, Int)]

    var messageEventCount = 0L
    var newFriendEventCount = 0L

    for (batchNum <- 0 until numBatches) {

      val batchFn = s"$baseFn-$batchNum"
      val filenames = (0 until numPartitions).map(i => s"$batchFn-$i")

      println("About to load more events...")

      val events = KryoLoad.loadFiles(sc, filenames)
      val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
      val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

      messageEventCount += messageEvents.count()
      newFriendEventCount += newFriendEvents.count()

      println(s"messageEventCount, newFriendEventCount $messageEventCount, $newFriendEventCount")

      val newFriendships = newFriendEvents.flatMap((nfe: NewFriendshipEvent) => {
        List((nfe.userIdA, nfe.userIdB) -> new TSValList((nfe.ts, 1)),
          (nfe.userIdB, nfe.userIdA) -> new TSValList((nfe.ts, 1)))
      })

      println(s"newFriendships ${newFriendships.count()}")

      val joinRes = allFriendships.fullOuterJoin(newFriendships)

      println(s"JoinRes ${joinRes.count()}")

      allFriendships = joinRes
        .mapValues({
          case (Some(a), Some(b)) => a.merge(b, _ + _)
          case (Some(a), None) => a
          case (None, Some(b)) => b
          case _ => throw new IllegalArgumentException
        })

      println(s"allFriendships ${allFriendships.count()}")

      val msgsWithTs = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> me.ts)
      // Combine together messages from the same user to cut down on the amount of joining done
      val msgsWithTsAggArrayBuffer = msgsWithTs.aggregateByKey(ArrayBuffer[Long]())((buf, v) => {
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

      println(s"msgsWithTsAggArrayBuffer ${msgsWithTsAggArrayBuffer.count()}")

      val msgsWithTsAgg = msgsWithTsAggArrayBuffer.mapValues(arrayBuf => new TSValList(arrayBuf.map(_ -> 1).toArray:_*))

      println(s"msgsWithTsAgg ${msgsWithTsAgg.count()}")

      val usersWithMsgSentToFriendOrNot = msgsWithTsAgg.leftOuterJoin(allFriendships)
        .map({
          case ((sendID, _), (sendTs, tsValListOption)) => sendID -> sendTs.evaluateAgainst[Int, (Int, Int)](tsValListOption, {
            case (_, Some(friendVal)) => if (friendVal == 0) (0, 1) else (1, 0)
            case (_, None) => (0, 1)
          })
        }) // first just map to sent to friend or not, then combine those forward

      println(s"usersWithMsgSentToFriendOrNot ${usersWithMsgSentToFriendOrNot.count()}")

      // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
      val usersWithMsgSentToFriendOrNotAgg = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
        val outList = new TSValList[(Int, Int)]()
        // basically do a k-way sorted list merge
        val queues = mutable.PriorityQueue()(new BufferTimestampOrdering[(Int, Int)])
        queues ++= listOfQueues.map(_.list)
        while (queues.nonEmpty) {
          val nextQueue = queues.dequeue()
          val next = nextQueue.head
          if (nextQueue.tail.nonEmpty) {
            queues.enqueue(nextQueue.tail)
          }
          outList.list.append(next)
        }
        outList
      })

      println(s"usersWithMsgSentToFriendOrNotAgg ${usersWithMsgSentToFriendOrNotAgg.count()}")

      userFriendMessageCounts = userFriendMessageCounts.fullOuterJoin(usersWithMsgSentToFriendOrNotAgg).mapValues({
        case (Some(a), Some(b)) => a.merge(b, (ct1, ct2) => (ct1._1+ct2._1, ct1._2+ct2._2))
        case (Some(a), None) => a
        case (None, Some(b)) => b.sumOverIncremental((0, 0), (ct1, ct2) => (ct1._1+ct2._1, ct1._2+ct2._2))
        case _ => throw new IllegalArgumentException
      })

      println(s"userFriendMessageCounts ${userFriendMessageCounts.count()}")

      val newSpamCounts = usersWithMsgSentToFriendOrNotAgg.leftOuterJoin(userFriendMessageCounts)
        .mapValues({
          case (messageSends, Some(sendCounts)) =>
            messageSends.evaluateAgainst[(Int, Int), Boolean](Some(sendCounts), (_, sendCount) => sendCount match {
              case Some((f, nf)) => f + nf > 5 && nf > 2 * f
              case _ => false
            })
          case (messageSends, None) => new TSValList((0, 0))
        })
        .mapValues(_.list.count(_._2 == true))

      println(s"newSpamCounts ${newSpamCounts.count()}")

      spamCountByUser = spamCountByUser.fullOuterJoin(newSpamCounts)
        .mapValues({
          case (Some(a), Some(b)) => a + b
          case (Some(a), None)    => a
          case (None, Some(b))    => b
          case _ => throw new IllegalArgumentException
        })
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


    //    val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
//    val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])
//
//    if (printDebug) {
//      println(s"Message count ${messageEvents.count()} // newFriendEvent count ${newFriendEvents.count()}")
//      println(s"Number of distinct users is ${messageEvents.map(_.senderUserId).distinct().count()}")
//    }
//
//    // for now just keep both friendship twice for simplicity sake
//    val friendships = newFriendEvents.flatMap((nfe: NewFriendshipEvent) => {
//      List((nfe.userIdA, nfe.userIdB) -> List((nfe.ts, 1)),
//        (nfe.userIdB, nfe.userIdA) -> List((nfe.ts, 1)))
//    }) // mapping (id, id) -> List[(ts, val)] pairs
//
//    if (printDebug) println(s"Size of friendships RDD: ${friendships.count()}")
//
//    // decide, on each message send, whether it was sent to a friend or not
//    val msgsWithTs = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> me.ts)
//    if (printDebug) println(s"Size of msgsWithTs RDD: ${msgsWithTs.count()}")
//
//
//    // Combine together messages from the same user to cut down on the amount of joining done
//    val msgsWithTsAgg = msgsWithTs.aggregateByKey(ArrayBuffer[Long]())((buf, v) => {
//      buf.insert(buf.lastIndexWhere(_ < v) + 1, v)
//      buf
//    }, (buf1, buf2) => {
//      var idx1 = 0
//      var idx2 = 0
//      val outBuf = new ArrayBuffer[Long](buf1.length + buf2.length)
//      while (idx1 < buf1.length || idx2 < buf2.length) {
//        if (idx1 == buf1.length) {
//          outBuf.append(buf2(idx2))
//          idx2 += 1
//        } else if ((idx2 == buf2.length) || (buf1(idx1) < buf2(idx2))) {
//          outBuf.append(buf1(idx1))
//          idx1 += 1
//        } else {
//          outBuf.append(buf2(idx2))
//          idx2 += 1
//        }
//      }
//      outBuf
//    })
//
//    if (printDebug) println(s"Size of msgsWithTsAgg: ${msgsWithTsAgg.count()}, " +
//      s"message count is ${msgsWithTsAgg.map(_._2).map(_.size).sum()}")
//
//     val usersWithMsgSentToFriendOrNot = msgsWithTsAgg.leftOuterJoin(friendships).map({
//      case ((idA, idB), (sortedTs, Some(list))) =>
//        var sortedList = Queue[(Long, (Int, Int))]()
//        val iteratorTs = sortedTs.toIterator
//        var currentListIndex = 0
//        var currentFriendshipValue = 0
//        while (iteratorTs.hasNext) {
//          val ts = iteratorTs.next()
//          while (currentListIndex < list.size && ts > list(currentListIndex)._1) {
//            currentFriendshipValue = list(currentFriendshipValue)._2
//            currentListIndex += 1
//          }
//          sortedList = sortedList :+ ts -> (if (currentFriendshipValue > 0) {
//            (1, 0) // Friends
//          } else {
//            (0, 1) // Not friends
//          })
//        }
//        idA -> sortedList
//      case ((idA, idB), (sortedTs, None)) => // At no point were they ever friends
//        var sortedList = Queue[(Long, (Int, Int))]()
//        for (ts <- sortedTs) {
//          sortedList = sortedList :+ ts -> (0, 1)
//        }
//        idA -> sortedList
//    })
//
//    if (printDebug) println(s"Size of usersWithMsgSentToFriendOrNot is ${usersWithMsgSentToFriendOrNot.count()}, " +
//      s"number of messages is ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_ => true)).map(_._2).sum()} " +
//      s"with ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_._2._1 == 1)).map(_._2).sum()} sent to friends " +
//      s"and ${usersWithMsgSentToFriendOrNot.mapValues(_.count(_._2._2 == 1)).map(_._2).sum()} to nonfriends")
//
//    // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
//    val usersWithMsgSendCts = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
//      var outList = Queue[(Long, (Int, Int))]()
//      val queues = mutable.PriorityQueue()(new Ordering[Queue[(Long, (Int, Int))]] {
//        def compare(x: Queue[(Long, (Int, Int))], y: Queue[(Long, (Int, Int))]): Int = {
//          -1 * x.head._1.compare(y.head._1)
//        }
//      })
//      queues ++= listOfQueues
//      var runningTotal = (0, 0)
//      while (queues.nonEmpty) {
//        val nextQueue = queues.dequeue()
//        val (next, q) = nextQueue.dequeue
//        if (q.nonEmpty) {
//          queues.enqueue(q)
//        }
//        runningTotal = (runningTotal._1 + next._2._1, runningTotal._2 + next._2._2)
//        outList = outList :+ next._1 -> runningTotal
//      }
//      outList
//    })
//
//    if (printDebug) println(s"Size of usersWithMsgSendCts is ${usersWithMsgSendCts.count()}, " +
//      s"Number of messages: ${usersWithMsgSendCts.mapValues(_.count(_ => true)).map(_._2).sum()}")
//
//    val spamCtByUser = usersWithMsgSendCts.mapValues(_.count({
//      case (_, (friendCt, nonfriendCt)) =>
//        friendCt + nonfriendCt > 5 && nonfriendCt > 2 * friendCt
//    }))
//    if (printDebug) {
//      println(s"Number of users: ${spamCtByUser.count()}")
//      println(s"Top 20 spammers: ${spamCtByUser.takeOrdered(20)(new Ordering[(Long, Int)] {
//        def compare(x: (Long, Int), y: (Long, Int)) = -1 * x._2.compare(y._2)
//      }).mkString(", ")}")
//    }
//
//    val spamCount = spamCtByUser.map(_._2).sum()
//
//    println(s"Final spam count is: $spamCount from ${spamCtByUser.filter(_._2 > 0).count()} users")
  }

}

