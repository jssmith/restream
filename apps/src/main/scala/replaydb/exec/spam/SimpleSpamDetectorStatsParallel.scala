package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._


/*
RULES:

1. spam if sent messages to non friends > 2 * (messages to friends) and total messages > 5

 */

class SimpleSpamDetectorStatsParallel(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) {
  import replayStateFactory._

  val friendships: ReplayMap[UserPair, Int] = getReplayMap(0)
  val friendSendRatio: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
  val messageSpamRatings: ReplayTimestampLocalMap[Long, Int] = getReplayTimestampLocalMap(0)
  val spamCounter: ReplayCounter = getReplayCounter

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(friendships, friendSendRatio, messageSpamRatings, spamCounter)
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
    // RULE 1 EVALUATION
    bind {
      me: MessageEvent =>
        friendSendRatio.get(ts = me.ts, key = me.senderUserId) match {
          case Some((toFriends, toNonFriends)) =>
            if (toFriends + toNonFriends > 5) {
              if (toNonFriends > 2 * toFriends) {
                //                  spamCounter.add(1, ts)
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
      e: PrintSpamCounter => println(s"spam count is ${spamCounter.get(e.ts)}")
    }
  }
}
