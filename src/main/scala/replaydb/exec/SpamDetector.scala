package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.event.{MessageEvent, NewFriendshipEvent, Event}
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

import scala.collection.mutable

object SpamDetector extends App {

  class Stats {
    val userSends = new mutable.HashMap[Long, (Long, Long)]
    val friendships = new mutable.HashSet[(Long,Long)]
    val spamUsers = new mutable.HashMap[Long, Long]

    var totalMessages = 0L
    var spamMessages: List[Event] = Nil
    
    def checkFriendship(uidA: Long, uidB: Long) = {
      friendships.contains(getFriendPair(uidA, uidB))
    }
    def addFriendship(uidA: Long, uidB: Long): Unit = {
      friendships += getFriendPair(uidA, uidB)
    }
    private def getFriendPair(uidA: Long, uidB: Long) = {
      if (uidA < uidB) (uidA, uidB) else (uidB, uidA)
    }
    
    def apply(e: Event): Unit = e match {
      case e: NewFriendshipEvent => addFriendship(e.userIdA, e.userIdB)
      case e: MessageEvent => {
        var userSend: (Long, Long) = userSends.get(e.senderUserId) match {
          case Some(sends) => sends
          case None => (0, 0)
        }
        userSend = if (checkFriendship(e.senderUserId, e.recipientUserId)) {
          (userSend._1 + 1L, userSend._2)
        } else {
          (userSend._1, userSend._2 + 1L)
        }
        if (userSend._2 > 5 && userSend._2 > userSend._1) {
          spamMessages = e :: spamMessages
          spamUsers.put(e.senderUserId, spamUsers.get(e.senderUserId) match {
            case Some(cnt) => cnt + 1
            case None => 1
          })
        }
        userSends.put(e.senderUserId, userSend)
      }
    }
    
    def printSpamMessages(): Unit = {
      spamMessages.foreach(println(_))
    }

    def printSpamUsers(): Unit = {
      for ((uid, spamCount) <- spamUsers) {
        val (friendSends, nonfriendSends) = userSends.get(uid) match {
          case Some(sends) => sends
        }
        println(s"$spamCount spam messages detected for user $uid, sent ${friendSends+nonfriendSends} total messages; " +
          s"$friendSends to friends and $nonfriendSends to nonfriends")
      }
    }
  }
  
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val stats = new Stats
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => {
    stats(e)
    pm.increment()
  })
//  stats.printSpamMessages()
  stats.printSpamUsers()
  pm.finished()
}
