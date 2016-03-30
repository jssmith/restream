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

object IpSpamDetectorSpark {

  // Takes (Long, A) pairs and sorts them by the first member of
  // the tuple (the timestamp)
  class TimestampOrdering[A] extends Ordering[(Long, A)] {
    def compare(x: (Long, A), y: (Long, A)): Int = x._1.compare(y._1)
  }

  val conf = new SparkConf().setAppName("ReStream Example Over Spark Testing").setMaster("local[4]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryoserializer.buffer.max", "250m")
//  conf.set("spark.kryo.registrationRequired", "true")
//  conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.TreeSet," +
//    "scala.collection.mutable.WrappedArray")
  val sc = new SparkContext(conf)

  private val emailRegex = """[\w-_\.+]*[\w-_\.]\@([\w]+\.)+[\w]+[\w]""".r
  def hasEmail(msg: String): Boolean = {
    emailRegex.pattern.matcher(msg).find()
  }

  def main(args: Array[String]) {

    if (args.length < 2 || args.length > 3) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar baseFilename numPartitions [ printDebug=false ]
          |  Example values:
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    printDebug     = true
        """.stripMargin)
      System.exit(1)
    }

    val baseFn = args(0)
    val numPartitions = args(1).toInt
    val printDebug = if (args.length > 2 && args(2) == "true") true else false
    val filenames = (0 until numPartitions).map(i => s"$baseFn-$i").toArray

    val events = loadFiles(filenames)

    val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
    val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

    if (printDebug) {
      println(s"Message count ${messageEvents.count()} // newFriendEvent count ${newFriendEvents.count()}")
      println(s"Number of distinct users is ${messageEvents.map(_.senderUserId).distinct().count()}")
    }

    //
    // GET A MAP OF  MessageID -> (IPemailSendCount, IPnonEmailSendCount)
    //
    val msgsWithTsIp = messageEvents.map(me => me.senderIp ->
      (me.ts, me.messageId, hasEmail(me.content)))

    // Aggregate by IP as well
    val msgsWithTsIpAgg = msgsWithTsIp.aggregateByKey(ArrayBuffer[(Long, Long, Boolean)]())((buf, v) => {
      buf.insert(buf.lastIndexWhere(_._1 < v._1) + 1, v)
      buf
    }, (buf1, buf2) => {
      var idx1 = 0
      var idx2 = 0
      val outBuf = new ArrayBuffer[(Long, Long, Boolean)](buf1.length + buf2.length)
      while (idx1 < buf1.length || idx2 < buf2.length) {
        if (idx1 == buf1.length) {
          outBuf.append(buf2(idx2))
          idx2 += 1
        } else if ((idx2 == buf2.length) || (buf1(idx1)._2 < buf2(idx2)._2)) {
          outBuf.append(buf1(idx1))
          idx1 += 1
        } else {
          outBuf.append(buf2(idx2))
          idx2 += 1
        }
      }
      outBuf
    })

    val ipAddressToSendCount = msgsWithTsIpAgg.mapValues(buf => {
      var runningTotal = (0, 0)
      buf.map({case (ts, mId, hasEmail) =>
        runningTotal = if (hasEmail) {
          (runningTotal._1 + 1, runningTotal._2)
        } else {
          (runningTotal._1, runningTotal._2 + 1)
        }
        (ts, mId, runningTotal)
      }).toArray
    })

    val msgIdsToIpSend = ipAddressToSendCount.flatMap({case (ip, buf) =>
      buf.map({case (ts, mId, sendCt) =>
        mId -> sendCt
      })
    })


    //
    // GET A MAP OF MessageID -> (userFriendSendCount, userNonFriendSendCount)
    // (same as SimpleSpamDetector except we have to carry the message ID through
    //  everything to use it later, and we flip the mapping at the end)
    //

    // for now just keep both friendship twice for simplicity sake
    val friendships = newFriendEvents.flatMap((nfe: NewFriendshipEvent) => {
      List((nfe.userIdA, nfe.userIdB) -> List((nfe.ts, 1)),
        (nfe.userIdB, nfe.userIdA) -> List((nfe.ts, 1)))
    }) // mapping (id, id) -> List[(ts, val)] pairs

    // decide, on each message send, whether it was sent to a friend or not
    val msgsWithTsUser = messageEvents.map(me => (me.senderUserId, me.recipientUserId) -> (me.ts, me.messageId))

    // Combine together messages from the same user to cut down on the amount of joining done
    val msgsWithTsUserAgg = msgsWithTsUser.aggregateByKey(ArrayBuffer[(Long, Long)]())((buf, v) => {
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
        } else if ((idx2 == buf2.length) || (buf1(idx1)._2 < buf2(idx2)._2)) {
          outBuf.append(buf1(idx1))
          idx1 += 1
        } else {
          outBuf.append(buf2(idx2))
          idx2 += 1
        }
      }
      outBuf
    })

    val usersWithMsgSentToFriendOrNot = msgsWithTsUserAgg.leftOuterJoin(friendships).map({
      case ((idA, idB), (sortedTs, Some(list))) =>
        var sortedList = Queue[(Long, Long, (Int, Int))]()
        val iteratorTs = sortedTs.toIterator
        var currentListIndex = 0
        var currentFriendshipValue = 0
        while (iteratorTs.hasNext) {
          val (ts, mId) = iteratorTs.next()
          while (currentListIndex < list.size && ts > list(currentListIndex)._1) {
            currentFriendshipValue = list(currentFriendshipValue)._2
            currentListIndex += 1
          }
          sortedList = sortedList :+ ((ts, mId, if (currentFriendshipValue > 0) {
            (1, 0) // Friends
          } else {
            (0, 1) // Not friends
          }))
        }
        idA -> sortedList
      case ((idA, idB), (sortedTs, None)) => // At no point were they ever friends
        var sortedList = Queue[(Long, Long, (Int, Int))]()
        for ((ts, mId) <- sortedTs) {
          sortedList = sortedList :+ ((ts, mId, (0, 1)))
        }
        idA -> sortedList
    })

    // map of uid -> many (ts, (friendSendCt, nonfriendSendCt)
    val usersWithMsgSendCts = usersWithMsgSentToFriendOrNot.groupByKey().mapValues(listOfQueues => {
      var outList = Queue[(Long, Long, (Int, Int))]()
      val queues = mutable.PriorityQueue()(new Ordering[Queue[(Long, Long, (Int, Int))]] {
        def compare(x: Queue[(Long, Long, (Int, Int))], y: Queue[(Long, Long, (Int, Int))]): Int = {
          -1 * x.head._1.compare(y.head._1)
        }
      })
      queues ++= listOfQueues
      var runningTotal = (0, 0)
      while (queues.nonEmpty) {
        val nextQueue = queues.dequeue()
        val (next, q) = nextQueue.dequeue
        if (q.nonEmpty) {
          queues.enqueue(q)
        }
        runningTotal = (runningTotal._1 + next._3._1, runningTotal._2 + next._3._2)
        outList = outList :+ ((next._1, next._2, runningTotal))
      }
      outList.toArray
    })

    val msgIdsToFriendSend = usersWithMsgSendCts.flatMap({case (sendId, buf) =>
        buf.map({case (ts, mId, sendCt) =>
            mId -> sendCt
        })
    })

    val spamMsgIds = msgIdsToIpSend.join(msgIdsToFriendSend).mapValues({
      case ((email, noEmail), (friend, nonFriend)) =>
        email.toDouble / (email + noEmail).toDouble > 0.2 &&
        friend + nonFriend > 5 && nonFriend > 2 * friend
    }).filter(_._2)

    println(s"Total number of spam messages is ${spamMsgIds.count()}")
  }



  def loadFiles(filenames: Seq[String]): RDD[Event] = {
    val instantiator = new ScalaKryoInstantiator
    def load(filename: String): Iterator[Event] = {
      val kryo = instantiator.newKryo()
      kryo.register(classOf[MessageEvent])
      kryo.register(classOf[NewFriendshipEvent])

      new Iterator[Event] {
        val input = new Input(new BufferedInputStream(new FileInputStream(filename)))
        var done = false
        var nextEvent: Event = null

        override def next: Event = {
          hasNext
          val retEvent = nextEvent
          nextEvent = null
          retEvent
        }

        override def hasNext: Boolean = {
          if (done) {
            return false
          }
          if (nextEvent == null) {
            try {
              nextEvent = kryo.readClassAndObject(input).asInstanceOf[Event]
            } catch {
              case e: KryoException =>
                input.close()
                done = true
            }
          }
          nextEvent != null
        }
      }
    }
    val partitions = filenames.length
    sc.parallelize(filenames, partitions).flatMap(load)
  }
}

