package replaydb.exec

import java.io._

import org.apache.commons.math3.stat.regression.SimpleRegression
import replaydb.event.{NewFriendshipEvent, Event, MessageEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

import scala.collection.mutable

object EventStats extends App {
  class MessageStats {
    class UserCounter {
      val userFreq = new mutable.HashMap[Long,Int]()
      def countUser(userId: Long): Unit = {
        userFreq.get(userId) match {
          case Some(ct) => userFreq.put(userId, ct + 1)
          case None => userFreq.put(userId, 1)
        }
      }
      def numUsers(): Int = {
        userFreq.size
      }
      def countFrequencies() = {
        userFreq.groupBy(_._2).mapValues(_.size).toArray.sortBy(-_._1)
      }
      def regression(): Unit = {
        val total = userFreq.values.sum.toDouble
        val probabilities = userFreq.values.map(_/total).toArray.sortBy(-_)
        // Write the histogram of activities
        var ind = 1
        var ct = 0
        val sr = new SimpleRegression
        while (ind <= probabilities.length) {
          sr.addData(Math.log(ind), Math.log(probabilities(ind-1)))
          ind += Math.max(1, (ind * 0.1).toInt)
          ct += 1
        }
        val zipfAlphaEstimate = -sr.getSlope
        val alphaEstimate = 1D + 1D / zipfAlphaEstimate
        println(s"\u03b1=$alphaEstimate b=$zipfAlphaEstimate")

      }
      def regression2(): Unit = {
        val minActivity = 5D // setting this to 1 leads to artifacts
        val activities = userFreq.values.filter(_>=minActivity).toArray
        val n = activities.length.toDouble
        val alphaEstimate = 1D + n / activities.map(p => Math.log(p/minActivity)).sum
        val zipfAlphaEstimate = 1D / (alphaEstimate - 1D)
        println(s"MLE estimate \u03b1=$alphaEstimate b=$zipfAlphaEstimate")
      }
      def saveActivities(fn: String): Unit = {
        val w = new PrintWriter(new BufferedOutputStream(new FileOutputStream(fn)))
        try {
          userFreq.values.foreach(ct => w.println(ct))
        } finally {
          w.close()
        }
      }
    }
    class InvalidCounter {
      var invalidCt = 0
      def countUser(userId: Long): Unit = {
        if (userId <= 0) {
          invalidCt += 1
        }
      }
      def hasInvalid: Boolean = {
        invalidCt > 0
      }
      def numInvalid = invalidCt
    }
    val sendersActive = new UserCounter
    val recipientsActive = new UserCounter
    val allActive = new UserCounter
    val invalidCounter = new InvalidCounter
    val friendships = new mutable.HashSet[(Long,Long)]()
    var allEventCt = 0L
    var allMessageCt = 0L
    var friendMessageCt = 0L
    def orderFriends(userIdA: Long, userIdB: Long): (Long, Long) = {
      if (userIdA < userIdB) {
        (userIdA, userIdB)
      } else {
        (userIdB, userIdA)
      }
    }
    def apply(e: Event): Unit = {
      e match {
        case me: MessageEvent =>
          sendersActive.countUser(me.senderUserId)
          recipientsActive.countUser(me.recipientUserId)
          allActive.countUser(me.senderUserId)
          allActive.countUser(me.recipientUserId)
          invalidCounter.countUser(me.senderUserId)
          invalidCounter.countUser(me.recipientUserId)
          if (friendships.contains(orderFriends(me.senderUserId, me.recipientUserId))) {
            friendMessageCt += 1
          }
          allMessageCt += 1
        case nf: NewFriendshipEvent =>
          invalidCounter.countUser(nf.userIdA)
          invalidCounter.countUser(nf.userIdB)
          friendships += orderFriends(nf.userIdA, nf.userIdB)
        case _ =>
      }
      allEventCt += 1
    }
    def summarize(): Unit = {
      print(s"Events: $allEventCt\nUsers: ${allActive.numUsers()}\nFriendships: ${friendships.size}\n")
      print(s"Messages: $allMessageCt\nMessages to friends: $friendMessageCt\n")
    }
  }
  if (args.length != 1) {
    println(
      """Usage: EventStats filename
      """.stripMargin)
    System.exit(1)
  }
  var filename = args(0)

  val ms = new MessageStats
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream(filename)), e => {
    ms(e)
    pm.increment()
  })
  ms.sendersActive.regression()
  ms.sendersActive.regression2()
  ms.recipientsActive.regression()
  ms.recipientsActive.regression2()
  ms.allActive.regression()
  ms.allActive.regression2()
  ms.sendersActive.saveActivities(filename + ".sender-activity")
  ms.recipientsActive.saveActivities(filename + ".recipient-activity")
  ms.allActive.saveActivities(filename + ".all-activity")
  ms.summarize()
  pm.finished()
}
