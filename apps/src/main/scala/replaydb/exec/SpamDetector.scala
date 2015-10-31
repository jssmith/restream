package replaydb.exec

import java.io.{FileInputStream, BufferedInputStream}

import replaydb.event.{MessageEvent, NewFriendshipEvent, Event}
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev._
import replaydb.runtimedev.monotonicImpl.{ReplayCounterImpl, ReplayMapImpl}
import replaydb.util.ProgressMeter

import replaydb.runtimedev.ReplayRuntime._

object SpamDetector extends App {

  class UserPair(val a: Long, val b: Long) {
    override def equals(o: Any) = o match {
      case that: UserPair => (a == that.a && b == that.b) || (a == that.b && b == that.a)
      case _ => false
    }
    override def hashCode(): Int = {
      a.hashCode() + 25741 * b.hashCode()
    }
  }

  case class PrintSpamCounter(ts: Long) extends Event

  class Stats {
    val friendships: ReplayMap[UserPair, Int] = new ReplayMapImpl(0)
    val friendSendRatio: ReplayMap[Long, (ReplayCounter, ReplayCounter)] =
      new ReplayMapImpl((new ReplayCounterImpl, new ReplayCounterImpl))
    val spamCounter: ReplayCounter = new ReplayCounterImpl
    def update(e: Event) = emit(e) {
      bind { e: NewFriendshipEvent =>
        friendships.put(new UserPair(e.userIdA, e.userIdB), 1, e.ts)
      }
      bind { me: MessageEvent =>
        val ts = me.ts
        friendships.get(new UserPair(me.senderUserId, me.recipientUserId), ts) match {
          case Some(_) => friendSendRatio.update(me.senderUserId, _._1.add(1, ts), ts)
          case None => friendSendRatio.update(me.senderUserId, _._2.add(1, ts), ts)
        }
      }
      bind {
        me: MessageEvent =>
          val ts = me.ts
          friendSendRatio.get(me.senderUserId, ts) match {
            case Some((toFriendsCt, toNonFriendsCt)) =>
              val toFriends = toFriendsCt.get(ts)
              val toNonFriends = toNonFriendsCt.get(ts)
              if (toFriends + toNonFriends > 5) {
                if (toNonFriends > toFriends) {
                  spamCounter.add(1, ts)
                }
              }
            case None =>
          }
      }
      bind {
        e: PrintSpamCounter => println(s"spam count is ${spamCounter.get(e.ts)}")
      }
    }
  }

  val eventStorage = new SocialNetworkStorage
  val stats = new Stats
  var lastTimestamp = 0L
  val pm = new ProgressMeter(printInterval = 1000000, () => { stats.update(new PrintSpamCounter(lastTimestamp)); ""})
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => {
    stats.update(e)
    lastTimestamp = e.ts
    pm.increment()
  })
  //  stats.printSpamMessages()
//  stats.printSpamUsers()
  pm.finished()
}
