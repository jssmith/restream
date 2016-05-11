package replaydb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import replaydb.event.{MessageEvent, NewFriendshipEvent}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object IpSpamDetectorSparkExactBatches {

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

    var allFriendships: RDD[((Long, Long), TSValList[Int])] = sc.emptyRDD[((Long, Long), TSValList[Int])]
    var userFriendMessageCounts: RDD[(Long, TSValList[IntPair])] = sc.emptyRDD[(Long, TSValList[IntPair])]
    var ipMessageCounts: RDD[(Int, TSValList[IntPair])] = sc.emptyRDD[(Int, TSValList[IntPair])]
    var spamCount = 0L

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

      val msgsWithTs = messageEvents.map(me => (me.senderUserId, me.recipientUserId) ->(me.ts, me.messageId))
      // Combine together messages from the same user to cut down on the amount of joining done
      val msgsWithTsAgg = msgsWithTs.aggregateByKey(ArrayBuffer[(Long, Long)]())((buf, v) => {
        buf.insert(buf.lastIndexWhere(_._1 < v._1) + 1, v)
        buf
      }, (buf1, buf2) => {
        var idx1 = 0
        var idx2 = 0
        val outBuf = new ArrayBuffer[(Long, Long)](buf1.length + buf2.length)
        while (idx1 < buf1.length || idx2 < buf2.length) {
          if (idx1 == buf1.length) {
            outBuf.append(buf2(idx2))
            idx2 += 1
          } else if ((idx2 == buf2.length) || (buf1(idx1)._1 < buf2(idx2)._1)) {
            outBuf.append(buf1(idx1))
            idx1 += 1
          } else {
            outBuf.append(buf2(idx2))
            idx2 += 1
          }
        }
        outBuf
      }).mapValues(buf => TSValList[Long](buf.toArray: _*))

      val usersWithMsgSentToFriendOrNot = msgsWithTsAgg.leftOuterJoin(allFriendships)
        .map({
          case ((sendID, _), (messageIds, tsValListOption)) => sendID -> messageIds.evaluateAgainst[Int, (Long, IntPair)](tsValListOption, {
            case (mId, Some(friendVal)) => mId -> (if (friendVal == 0) (0, 1) else (1, 0))
            case (mId, None) => mId ->(0, 1)
          })
        }) // first just map to sent to friend or not, then combine those forward

      // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
      val usersWithMsgSentToFriendOrNotAgg = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
        val outList = new TSValList[(Long, IntPair)]()
        // basically do a k-way sorted list merge
        val queues = mutable.PriorityQueue()(new BufferTimestampOrdering[(Long, IntPair)])
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
        case (Some(a), Some(b)) => a.merge[(Long, IntPair)](b, { case (base, (mId, inc)) => addPairs[Int](base, inc) }).gcUpTo(lowestTs)
        case (Some(a), None) => a.gcUpTo(lowestTs)
        case (None, Some(b)) => b.sumOverIncremental((0, 0), { case (base, (mId, inc)) => addPairs[Int](base, inc) })
        case _ => throw new IllegalArgumentException
      })

      val friendSpamMessageIds = usersWithMsgSentToFriendOrNotAgg.leftOuterJoin(userFriendMessageCounts)
        .mapValues({ case (messages, sendCounts) => messages.evaluateAgainst[IntPair, (Long, IntPair)](sendCounts, (msg, sendCount) =>
          (msg, sendCount) match {
            case ((mId, _), Some((friendCt, nonfriendCt))) => mId ->(friendCt, nonfriendCt)
            case ((mId, _), None) => mId ->(0, 0)
          }
        )
        }).flatMap({ case (ip, tsValList) => tsValList.buf.map({ case (ts, (mId, sendCount)) => mId -> sendCount }).filter({
        case (mId, (friend, nonFriend)) => friend + nonFriend > 5 && nonFriend > 2 * friend
      }).map(_._1)
      })

      val ipSentEmailOrNot = messageEvents.map(me => me.senderIp ->(me.ts, me.messageId -> (if (hasEmail(me.content)) (1, 0) else (0, 1))))
        .groupByKey().mapValues(allValues => {
        val outList = new TSValList[(Long, IntPair)]()
        // basically do a k-way sorted list merge
        val queue = mutable.PriorityQueue()(new Ordering[TSVal[(Long, IntPair)]] {
          def compare(x: TSVal[(Long, IntPair)], y: TSVal[(Long, IntPair)]) = -1 * x._1.compare(y._1)
        })
        queue ++= allValues
        while (queue.nonEmpty) {
          val next = queue.dequeue()
          outList.buf.append(next)
        }
        outList
      })

      ipMessageCounts = ipMessageCounts.fullOuterJoin(ipSentEmailOrNot)
        .mapValues({
          case (Some(a), Some(b)) => a.merge[(Long, IntPair)](b, (base, inc) => addPairs[Int](base, inc._2)).gcUpTo(lowestTs)
          case (Some(a), None) => a.gcUpTo(lowestTs)
          case (None, Some(b)) => b.sumOverIncremental((0, 0), (base, inc) => addPairs[Int](base, inc._2))
          case _ => throw new IllegalArgumentException
        })

      val ipSpamMessageIds = ipSentEmailOrNot.leftOuterJoin(ipMessageCounts).mapValues({
        case (messages, sendCounts) => messages.evaluateAgainst[IntPair, (Long, IntPair)](sendCounts, (msg, sendCount) =>
          (msg, sendCount) match {
            case ((mId, _), Some((emailCt, nonEmailCt))) => mId ->(emailCt, nonEmailCt)
            case ((mId, _), None) => mId ->(0, 0)
          }
        )
      }).flatMap({ case (ip, tsValList) => tsValList.buf.map({ case (ts, (mId, sendCount)) => mId -> sendCount }).filter({
        case (mId, (email, noEmail)) => (email + noEmail > 0) && (email.toDouble / (email + noEmail).toDouble > 0.2)
      }).map(_._1)
      })

      spamCount += ipSpamMessageIds.intersection(friendSpamMessageIds).count()
    }
    
    println(s"FINAL SPAM COUNT: $spamCount")

    val processTime = System.currentTimeMillis() - startTime
    val eventCount = newFriendEventCount + messageEventCount
    println(s"Final runtime was $processTime ms (${processTime / 1000} sec)")
    println(s"Process rate was ${eventCount / (processTime / 1000)} per second")
    println(s"CSV,IpSpamDetectorSparkExactBatches,$numPartitions,$eventCount,$processTime,$numBatches")
  }

}

