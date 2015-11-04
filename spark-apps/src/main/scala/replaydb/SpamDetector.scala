package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{ScalaKryoInstantiator, Input}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import replaydb.event.{Event, NewFriendshipEvent, MessageEvent}
import org.apache.spark.sql.functions._

object SpamDetector {

//  val instantiator = new ScalaKryoInstantiator
//
//  val conf = new SparkConf().setAppName("SpamDetector").setMaster("local[4]")
//  val sc = new SparkContext(conf)
//
//  def main(args: Array[String]) {
////    val events = loadFiles(Seq("/tmp/events.out"))
//
//    val numPartitions = if (args.length >= 1) { args(0).toInt } else { 1 }
////    val fileInputs = if (numPartitions > 1)
////      for (i <- 0 until numPartitions) yield s"/tmp/events.out-$i"
////    else
////      Seq("/tmp/events.out")
////    val kryosWithInputs = fileInputs.map { filename =>
////      val kryo = instantiator.newKryo()
////      kryo.register(classOf[MessageEvent])
////      kryo.register(classOf[NewFriendshipEvent])
////      val input = new Input(new BufferedInputStream(new FileInputStream(filename)))
////      (kryo, input)
////    }
//    val kryo = instantiator.newKryo()
//    kryo.register(classOf[MessageEvent])
//    kryo.register(classOf[NewFriendshipEvent])
//    val input = new Input(new BufferedInputStream(new FileInputStream("/tmp/events.out")))
//
//    var stateRDD: RDD[(Long, State)] = sc.emptyRDD[(Long, State)]
//
//    val windowSize = 20000 // events
//    var done = false
//    while (!done) {
//      val events = for (i <- 0 until windowSize) yield {
//        try {
//          kryo.readClassAndObject(input).asInstanceOf[Event]
//        } catch {
//          case e: KryoException =>
//            input.close()
//            done = true
//            null // TODO this isn't quite correct
//        }
//      }
//
//      // collect all of the events for each `id`
//      val eventRDD = sc.parallelize(events, numPartitions).keyBy(_.id).groupByKey().cache()
//      // attach the state for each `id` to the associated events
//      val eventRDDWithState = eventRDD.fullOuterJoin(stateRDD)
//      stateRDD = eventRDDWithState.map { tuple =>
//        val id = tuple._1
//        val eventState= tuple._2
//        val events = eventState._1
//        val state = eventState._2
//        // TODO ^ that all needs to get cleaned up, clearly
//        events match {
//          case es: Iterable[Event] => {
//            val outputState = new State
//            for (e <- es) {
//
//              // process all event bindings
//              // update outputState
//            }
//            (id, outputState)
//          }
//          case None => (id, state.getOrElse { State.EmptyState }) // TODO correct?
//        }
//      }
//      // now state would include friendships (Phase I)
//
//      val eventRDDWithFriendships = eventRDD.fullOuterJoin(stateRDD)
//      stateRDD = ??? // repeat the map
//
//      val eventRDDWithFriendSendRatio = eventRDD.fullOuterJoin(stateRDD)
//      stateRDD = ??? // repeat the map
//
//      val
//
//
//      // join events with current state
//      // run job, get new state
//      // repeat
//    }
////    val messageEvents = events.filter(_.isInstanceOf[MessageEvent]).
////      map(_.asInstanceOf[MessageEvent]).cache()
////
////    val newFriendEvents = events.filter(_.isInstanceOf[NewFriendshipEvent]).
////      map(_.asInstanceOf[NewFriendshipEvent]).cache()
////
////    val grouped = messageEvents.groupBy("senderUserId", "recipientUserId").count()
//////    grouped.show()
//////    println("after grouping we have: " + grouped.count)
////    grouped.registerTempTable("msgs")
////
////    val allMsgsToFriends = hiveContext.sql("SELECT * FROM msgs JOIN nfes ON " +
////      "((msgs.senderUserId == nfes.userIdA AND msgs.recipientUserId == nfes.userIdB) " +
////      "OR (msgs.senderUserId == nfes.userIdB AND msgs.recipientUserId == nfes.userIdA))")
//////    println("messages to friends: " + allMsgsToFriends.count)
////
////    val msgsToFriends = allMsgsToFriends.groupBy("senderUserId").sum("count")
////    val msgsTotal = grouped.groupBy("senderUserId").sum("count")
////    val spammers = msgsToFriends.join(msgsTotal, "senderUserId").
////      where(msgsToFriends("sum(count)") < (msgsTotal("sum(count)") * 0.5))
////
////    println("There are " + spammers.count + " users who sent more messages to nonfriends " +
////      "than to friends (considering them a friend if they became friends at any point, " +
////      "not if they were friends when the message was sent)")
////    println("Total messages they sent to nonfriends: ")
////    spammers.select((msgsTotal("sum(count)") - msgsToFriends("sum(count)")).as("nonFriendMsgs")).
////      agg(sum($"nonFriendMsgs")).show
//
////    println("count: " + events.count + ", message: " + messageEventDF.count + ", friend: " + newFriendEventDF.count)
//  }
//
//  // store state about an id
//  class State {
//
//  }
//
//  object State {
//    val EmptyState = new State
//  }
//
//
//  def loadFiles(filenames: Seq[String]): RDD[Event] = {
//    def load(filename: String): Iterator[Event] = {
//      val kryo = instantiator.newKryo()
//      kryo.register(classOf[MessageEvent])
//      kryo.register(classOf[NewFriendshipEvent])
//
//      new Iterator[Event] {
//        val input = new Input(new BufferedInputStream(new FileInputStream(filename)))
//        var done = false
//        var nextEvent: Event = null
//
//        override def next: Event = {
//          hasNext
//          val retEvent = nextEvent
//          nextEvent = null
//          retEvent
//        }
//
//        override def hasNext: Boolean = {
//          if (done) {
//            return false
//          }
//          if (nextEvent == null) {
//            try {
//              nextEvent = kryo.readClassAndObject(input).asInstanceOf[Event]
//            } catch {
//              case e: KryoException =>
//                input.close()
//                done = true
//            }
//          }
//          nextEvent != null
//        }
//      }
//    }
//    val partitions = filenames.length
////    val kryos = for (i <- 0 until partitions) yield {
////      val kryo = instantiator.newKryo()
////      kryo.register(classOf[MessageEvent])
////      kryo.register(classOf[NewFriendshipEvent])
////      kryo
////    }
////    val filenamesWithKryos = kryos zip filenames
////    sc.parallelize(filenamesWithKryos, partitions).flatMapValues(load)
//    sc.parallelize(filenames, partitions).flatMap(load)
//  }
}

