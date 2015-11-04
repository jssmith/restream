package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{ScalaKryoInstantiator, Input}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import replaydb.event.{Event, NewFriendshipEvent, MessageEvent}
import org.apache.spark.sql.functions._

object SpamDetectorSparkSQL {

  val instantiator = new ScalaKryoInstantiator

  val conf = new SparkConf().setAppName("Spam Detector in SparkSQL").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._

  def main(args: Array[String]) {
    val numPartitions = if (args.length >= 1) { args(0).toInt } else { 1 }
    val fileInputs = if (numPartitions > 1)
      for (i <- 0 until numPartitions) yield s"/tmp/events.out-$i"
    else
      Seq("/tmp/events.out")
    val events = loadFiles(fileInputs)

    val msgDF = events.filter(_.isInstanceOf[MessageEvent]).
      map(_.asInstanceOf[MessageEvent]).toDF().cache()
    //    msgDF.registerTempTable("msgs")

    val nfeDF = events.filter(_.isInstanceOf[NewFriendshipEvent]).
      map(_.asInstanceOf[NewFriendshipEvent]).toDF().cache()

    val grouped = msgDF.groupBy("senderUserId", "recipientUserId").count()
    //    grouped.show()
    //    println("after grouping we have: " + grouped.count)

    val allMsgsToFriends = grouped.join(nfeDF,
      grouped("senderUserID")===nfeDF("userIdA") and grouped("recipientUserId")===nfeDF("userIdB"))
//        println("messages to friends: " + allMsgsToFriends.count)

    val msgsToFriends = allMsgsToFriends.groupBy("senderUserId").sum("count")
    val msgsTotal = grouped.groupBy("senderUserId").sum("count")
    val spammers = msgsToFriends.join(msgsTotal, "senderUserId").
      where(msgsToFriends("sum(count)") < (msgsTotal("sum(count)") * 0.5))

    println("There are " + spammers.count + " users who sent more messages to nonfriends " +
      "than to friends (considering them a friend if they became friends at any point, " +
      "not if they were friends when the message was sent)")
    println("Total messages they sent to nonfriends: ")
    spammers.select((msgsTotal("sum(count)") - msgsToFriends("sum(count)")).as("nonFriendMsgs")).
      agg(sum($"nonFriendMsgs")).show

    //    println("count: " + events.count + ", message: " + msgDF.count + ", friend: " + ndfDF.count)
  }

  def loadFiles(filenames: Seq[String]): RDD[Event] = {
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


