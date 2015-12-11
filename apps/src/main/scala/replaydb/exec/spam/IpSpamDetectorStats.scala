package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._
import replaydb.service.KryoCommandDecoder
import replaydb.service.driver.KryoCommands


/*
RULES:

1. ip is spammy if > 20% of messages contain e-mail address
2. message is spam if
        a: user sent messages to non friends > 2 * (messages to friends)
    and b: and total messages > 5
    and c: message is sent from spammy ip

 */

class IpSpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends RuntimeStats with HasSpamCounter {
  import replayStateFactory._

  val friendships: ReplayMap[UserPair, Int] = getReplayMap(0)
  val friendSendRatio: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
  val ipSendRatio: ReplayMap[Int, (Long, Long)] = getReplayMap((0L,0L))
  val spamCounter: ReplayCounter = getReplayCounter
  val messageSpamRatings: ReplayTimestampLocalMap[Long, Int] = getReplayTimestampLocalMap(0)

  registerClass(classOf[UserPair])

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(friendships, friendSendRatio, spamCounter, ipSendRatio, messageSpamRatings)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { e: NewFriendshipEvent =>
      friendships.merge(ts = e.ts, key = new UserPair(e.userIdA, e.userIdB), fn = _ => 1)
    }
    // RULE 1 STATE
    bind { me: MessageEvent =>
      friendships.get(ts = me.ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
        case Some(_) => friendSendRatio.merge(ts = me.ts, key = me.senderUserId, {case (friends, nonFriends) => (friends + 1, nonFriends)})
        case None => friendSendRatio.merge(ts = me.ts, key = me.senderUserId, {case (friends, nonFriends) => (friends, nonFriends + 1)})
      }
    }
    bind {
      me: MessageEvent =>
        val hasEmail = SpamUtil.hasEmail(me.content)
        if (hasEmail) {
          ipSendRatio.merge(ts = me.ts, key = me.senderIp, { case (hasEmailCt, hasNoEmailCt) => (hasEmailCt + 1, hasNoEmailCt) })
        } else {
          ipSendRatio.merge(ts = me.ts, key = me.senderIp, { case (hasEmailCt, hasNoEmailCt) => (hasEmailCt, hasNoEmailCt + 1) })
        }
    }
    // RULE 1 EVALUATION
    bind {
      me: MessageEvent =>
        val isIpSpammy = ipSendRatio.get(ts = me.ts, key = me.senderIp) match {
          case Some((hasEmailCt, hasNoEmailCt)) =>
            hasEmailCt.toDouble / (hasEmailCt + hasNoEmailCt).toDouble > 0.2
          case None => false
        }
        friendSendRatio.get(ts = me.ts, key = me.senderUserId) match {
          case Some((toFriends, toNonFriends)) =>
            if (toFriends + toNonFriends > 5) {
              if (toNonFriends > 2 * toFriends && isIpSpammy) {
                messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
              }
            }
          case None =>
        }
    }
    // AGGREGATE
    bind {
      me: MessageEvent =>
        messageSpamRatings.get(me.ts, me.messageId) match {
          case Some(score) => if (score >= SpamScoreThreshold) spamCounter.add(1, me.ts)
          case None =>
        }
    }
    bind {
      e: PrintSpamCounter => println(s"\n\nSPAM COUNT is ${spamCounter.get(e.ts)}\n\n")
    }
  }
}
