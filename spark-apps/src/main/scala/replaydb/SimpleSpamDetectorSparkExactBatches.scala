package replaydb

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import replaydb.event.{MessageEvent, NewFriendshipEvent}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SimpleSpamDetectorSparkExactBatches {

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
    var userFriendMessageCounts: RDD[(Long, TSValList[IntPair])] = sc.emptyRDD[(Long, TSValList[IntPair])]
    var spamCountByUser: RDD[(Long, Int)] = sc.emptyRDD[(Long, Int)]

    var messageEventCount = 0L
    var newFriendEventCount = 0L

    for (batchNum <- 0 until numBatches) {

      val batchFn = if (numBatches == 1) baseFn else s"$baseFn-$batchNum"
      val filenames = (0 until numPartitions).map(i => s"$batchFn-$i")

      val events = KryoLoad.loadFiles(sc, filenames)
      val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
      val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

      messageEventCount += messageEvents.count()
      newFriendEventCount += newFriendEvents.count()
      val lowestMessageTs = messageEvents.min()(new Ordering[MessageEvent] {
        def compare(x: MessageEvent, y: MessageEvent) = x.ts.compare(y.ts)
      }).ts
      val lowestFriendEventTs = newFriendEvents.min()(new Ordering[NewFriendshipEvent] {
        def compare(x: NewFriendshipEvent, y: NewFriendshipEvent) = x.ts.compare(y.ts)
      }).ts
      val lowestTs = Math.min(lowestMessageTs, lowestFriendEventTs)

      val newFriendships = newFriendEvents.flatMap((nfe: NewFriendshipEvent) => {
        List((nfe.userIdA, nfe.userIdB) -> TSValList((nfe.ts, 1)),
          (nfe.userIdB, nfe.userIdA) -> TSValList((nfe.ts, 1)))
      })

      allFriendships = allFriendships.fullOuterJoin(newFriendships)
        .mapValues({
          case (Some(a), Some(b)) => a.merge[Int](b, _ + _).gcUpTo(lowestTs)
          case (Some(a), None) => a.gcUpTo(lowestTs)
          case (None, Some(b)) => b
          case _ => throw new IllegalArgumentException
        })

      val msgsWithTs = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> me.ts)
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
      }).mapValues(buf => TSValList[Int](buf.map(_ -> 1).toArray:_*))

      val usersWithMsgSentToFriendOrNot = msgsWithTsAgg.leftOuterJoin(allFriendships)
        .map({
          case ((sendID, _), (sendTs, tsValListOption)) => sendID -> sendTs.evaluateAgainst[Int, IntPair](tsValListOption, {
            case (_, Some(friendVal)) => if (friendVal == 0) (0, 1) else (1, 0)
            case (_, None) => (0, 1)
          })
        }) // first just map to sent to friend or not, then combine those forward

      // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
      val usersWithMsgSentToFriendOrNotAgg = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
        val outList = new TSValList[IntPair]()
        // basically do a k-way sorted list merge
        val queues = mutable.PriorityQueue()(new BufferTimestampOrdering[IntPair])
        queues ++= listOfQueues.map(_.buf)
        while (queues.nonEmpty) {
          val nextQueue = queues.dequeue()
          val next = nextQueue.head
          if (nextQueue.tail.nonEmpty) {
            queues.enqueue(nextQueue.tail)
          }
          outList.buf.append(next)
        }
        outList
      })

      userFriendMessageCounts = userFriendMessageCounts.fullOuterJoin(usersWithMsgSentToFriendOrNotAgg).mapValues({
        case (Some(a), Some(b)) => a.merge[IntPair](b, addPairs[Int]).gcUpTo(lowestTs)
        case (Some(a), None) => a.gcUpTo(lowestTs)
        case (None, Some(b)) => b.sumOverIncremental((0, 0), addPairs[Int])
        case _ => throw new IllegalArgumentException
      })

      val newSpamCounts = usersWithMsgSentToFriendOrNotAgg.leftOuterJoin(userFriendMessageCounts)
        .mapValues({
          case (messageSends, Some(sendCounts)) =>
            messageSends.evaluateAgainst[IntPair, Boolean](Some(sendCounts), (_, sendCount) => sendCount match {
              case Some((f, nf)) => f + nf > 5 && nf > 2 * f
              case _ => false
            })
          case (messageSends, None) => TSValList((0, 0))
        })
        .mapValues(_.buf.count(_._2 == true))

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

    val processTime = System.currentTimeMillis() - startTime
    val eventCount = newFriendEventCount + messageEventCount
    println(s"Final runtime was $processTime ms (${processTime / 1000} sec)")
    println(s"Process rate was ${eventCount / (processTime / 1000)} per second")
    println(s"CSV,SimpleSpamDetectorSparkExactBatches,$numPartitions,$eventCount,$processTime,$numBatches")
  }

}

