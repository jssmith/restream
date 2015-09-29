package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.event.{NewFriendshipEvent, Event, MessageEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

import scala.collection.mutable

object EventStats extends App {
  class MessageStats {
    val userFreq = new mutable.HashMap[Long,Int]()
    val friendships = new mutable.HashSet[(Long,Long)]()
    var allEventCt = 0L
    var allMessageCt = 0L
    var friendMessageCt = 0L
    def countUser(userId: Long) = {
      userFreq.get(userId) match {
        case Some(ct) => userFreq.put(userId, ct + 1)
        case None => userFreq.put(userId, 1)
      }
    }
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
          countUser(me.senderUserId)
          countUser(me.recipientUserId)
          if (friendships.contains(orderFriends(me.senderUserId, me.recipientUserId))) {
            friendMessageCt += 1
          }
          allMessageCt += 1
        case nf: NewFriendshipEvent =>
          friendships += orderFriends(nf.userIdA, nf.userIdB)
        case _ =>
      }
      allEventCt += 1
    }
    def countUserFrequencies() = {
      // returns map of form (number of messages) => (number of users)
      userFreq.groupBy(_._2).mapValues(_.size).toArray.sortBy(-_._1)
    }
    def summarize(): Unit = {
      print(s"Events: $allEventCt\nUsers: ${userFreq.size}\nFriendships: ${friendships.size}\n")
      print(s"Messages: $allMessageCt\nMessages to friends: $friendMessageCt\n")
    }
  }
  val ms = new MessageStats
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => {
    ms(e)
    pm.increment()
  })
  ms.countUserFrequencies().foreach(x => println(s"${x._1} : ${x._2}"))
  ms.summarize()
  pm.finished()
}
