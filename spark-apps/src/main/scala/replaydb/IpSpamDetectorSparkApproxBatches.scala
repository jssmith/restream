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

    if (args.length < 3 || args.length > 4) {
      println(
        """Usage: ./spark-submit --class replaydb.SpamDetectorSpark app-jar baseFilename numPartitions numBatches [ printDebug=false ]
          |  Example values:
          |    baseFilename   = ~/data/events.out
          |    numPartitions  = 4
          |    numBatches     = 100
          |    printDebug     = true
        """.stripMargin)
      System.exit(1)
    }

    val baseFn = args(0)
    val numPartitions = args(1).toInt
    val numBatches = args(2).toInt
    val printDebug = if (args.length > 3 && args(3) == "true") true else false

    var batchNum = 0

    var spamCounts = 0L
    var allFriendships: RDD[((Long, Long), Int)] = sc.emptyRDD[((Long, Long), Int)]
    var userFriendMessageCounts: RDD[(Long, (Int, Int))] = sc.emptyRDD[(Long, (Int, Int))]
    var ipMessageCounts: RDD[(Int, (Int, Int))] = sc.emptyRDD[(Int, (Int, Int))]
//    var spamCountByUser: RDD[(Long, Int)] = sc.emptyRDD[(Long, Int)]

    for (i <- 0 until numBatches) {
      val batchFn = s"$baseFn-$batchNum"
      val filenames = (0 until numPartitions).map(i => s"$batchFn-$i").toArray
      val events = loadFiles(filenames)
      val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).map(_.asInstanceOf[MessageEvent])
      val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).map(_.asInstanceOf[NewFriendshipEvent])

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

      val ipSendCt = messageEvents.map(me => me.senderIp -> hasEmail(me.content))
        .aggregateByKey((0, 0))({
          case ((email, noEmail), hasEmail) =>
            if (hasEmail) { (email + 1, noEmail) } else { (email, noEmail + 1) }
        }, {case ((email1, noEmail1), (email2, noEmail2)) => (email1 + email2, noEmail1 + noEmail2)})

      ipMessageCounts = ipMessageCounts.fullOuterJoin(ipSendCt)
        .mapValues({
          case (Some((email1, noEmail1)), Some((email2, noEmail2))) => (email1 + email2, noEmail1 + noEmail2)
          case (Some(a), None) => a
          case (None, Some(a)) => a
          case _ => (0, 0)
        })

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

