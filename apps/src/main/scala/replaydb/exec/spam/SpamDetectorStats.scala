package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._
import replaydb.util.time._
import replaydb.event.Event

import scala.collection.immutable


/*
RULES:

1. spam if sent messages to non friends > 2 * (messages to friends) and total messages > 5
2. spam if you've sent > 7 messages to nonfriends in last 120 minutes with at least 5 unique recipients
3. spam if > 0.5 of your messages in the last 30 days contain an email address
4. spam if > 0.2 of your messages from the last 7 days were sent in the last day (and your first message was >= 7 days ago)
5. spam if < 0.1 of your messages for all time were sent as responses (defined as
  //         a message A->B when a message B->A exists within the last 7 days)

 */

class SpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface {
  import replayStateFactory._

  val friendships: ReplayMap[UserPair, Int] = getReplayMap(0)
  val friendSendRatio: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
  val spamCounter: ReplayCounter = getReplayCounter
  val nonfriendMessagesInLastInterval: ReplayMap[Long, Long] = getReplayMap(0)
  // Mapping userIDa -> (userIDb -> # messages sent userIDa to userIDb in last NonfriendMessageCountInterval)
  val uniqueNonfriendsSentToInLastInterval: ReplayMap[Long, immutable.Map[Long, Long]] =
    getReplayMap(new immutable.HashMap)
  val messageContainingEmailFraction: ReplayMap[Long, (Long, Long)] = getReplayMap((0L, 0L))
  val messagesFractionLast7DaysInLast24Hours: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
  val userFirstMessageTS: ReplayMap[Long, Long] = getReplayMap(Long.MaxValue)
  val userMostRecentReceivedMessage: ReplayMap[Long, immutable.Map[Long, Long]] =
    getReplayMap(new immutable.HashMap)
  val messageSentInResponseFraction: ReplayMap[Long, (Long, Long)] = getReplayMap((0L, 0L))
  val messageSpamRatings: ReplayTimestampLocalMap[Long, Int] = getReplayTimestampLocalMap(0)

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(friendships, friendSendRatio, spamCounter, messageSpamRatings, uniqueNonfriendsSentToInLastInterval,
      nonfriendMessagesInLastInterval, messageContainingEmailFraction, messagesFractionLast7DaysInLast24Hours,
      userFirstMessageTS, userMostRecentReceivedMessage, messageSentInResponseFraction)
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
    // RULE 2 STATE
    bind {
      me: MessageEvent =>
        friendships.get(ts = me.ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
          case Some(_) =>
          case None =>
            nonfriendMessagesInLastInterval.merge(me.ts, me.senderUserId, _ + 1)
            nonfriendMessagesInLastInterval.merge(me.ts + NonfriendMessageCountInterval, me.senderUserId, _ - 1)
            uniqueNonfriendsSentToInLastInterval.merge(me.ts, me.senderUserId,
              map => map.updated(me.recipientUserId, map.getOrElse(me.recipientUserId, 0L) + 1))
            uniqueNonfriendsSentToInLastInterval.merge(me.ts + NonfriendMessageCountInterval, me.senderUserId,
              map => { val x = map.getOrElse(me.recipientUserId, 0L); if (x > 0) map.updated(me.recipientUserId, x - 1) else null })
              // null case should never happen - leave null to throw NPE
        }
    }
    // RULE 2 EVALUATION
    bind {
      // if sent more than NonfriendMessageCountSpamThreshold messages to > UniqueNonfriendCountSpamThreshold
      // distinct nonfriends in last NonfriendMessageCountInterval, spam
      // NOTE: these numbers should probably be higher (and the time period shorter) but our current
      //       generator doesn't actually create any messages matching this if I turn them up
      me: MessageEvent =>
        val msgCnt = nonfriendMessagesInLastInterval.get(me.ts, me.senderUserId) match {
          case Some(cnt) => cnt
          case None => 0
        }
        if (msgCnt > NonfriendMessageCountSpamThreshold) {
          val nonfriendCnt = uniqueNonfriendsSentToInLastInterval.get(me.ts, me.senderUserId) match {
            case Some(friendMap) => friendMap.count(_._2 > 0)
            case None => 0
          }
          if (nonfriendCnt > UniqueNonfriendCountSpamThreshold) {
            messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
          }
        }
    }
    // RULE 3 STATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        if (containsEmail(me.content)) {
          messageContainingEmailFraction.merge(ts, me.senderUserId, (ratio) => (ratio._1 + 1, ratio._2 + 1))
          messageContainingEmailFraction.merge(ts + MessageContainingEmailInterval,
            me.senderUserId, (ratio) => (ratio._1 - 1, ratio._2 - 1))
        } else {
          messageContainingEmailFraction.merge(ts, me.senderUserId, (ratio) => (ratio._1, ratio._2 + 1))
          messageContainingEmailFraction.merge(ts + MessageContainingEmailInterval,
            me.senderUserId, (ratio) => (ratio._1, ratio._2 - 1))
        }
    }
    // RULE 3 EVALUATION
    bind {
      me: MessageEvent =>
        messageContainingEmailFraction.get(me.ts, me.senderUserId) match {
          case Some((email: Long, all: Long)) =>
            if ((email + 0.0) / all > MessageContainingEmailFractionThreshold) {
              messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
            }
          case None =>
        }
    }
    // RULE 4 STATE
    bind {
      me: MessageEvent =>
        userFirstMessageTS.merge(me.ts, me.senderUserId, Math.min(_, me.ts)) // kind of wasteful...
        messagesFractionLast7DaysInLast24Hours.merge(me.ts, me.senderUserId, old => (old._1 + 1, old._2 + 1))
        messagesFractionLast7DaysInLast24Hours.merge(me.ts + 1.days, me.senderUserId, old => (old._1 - 1, old._2))
        messagesFractionLast7DaysInLast24Hours.merge(me.ts + 7.days, me.senderUserId, old => (old._1, old._2 - 1))
    }
    // RULE 4 EVALUATE
    bind {
      me: MessageEvent =>
        val userExistedMoreThan7Days = userFirstMessageTS.get(me.ts, me.senderUserId) match {
          case Some(firstTS) => firstTS > me.ts - 7.days
          case None => false
        }
        if (userExistedMoreThan7Days) {
          messagesFractionLast7DaysInLast24Hours.get(me.ts, me.senderUserId) match {
            case Some((last24Hours, last7Days)) =>
              if ((last24Hours + 0.0) / last7Days > MessagesLast7DaysSentLast24HoursFraction) {
                messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
              }
            case None =>
          }
        }
    }
    // RULE 5 STATE 1
    bind {
      me: MessageEvent =>
        userMostRecentReceivedMessage.merge(me.ts, me.recipientUserId, map => {
          map.updated(me.senderUserId, me.ts)
        })
    }
    // RULE 5 STATE 2
    bind {
      me: MessageEvent =>
        val isResponse = userMostRecentReceivedMessage.get(me.ts, me.senderUserId) match {
          case Some(map) => map.getOrElse(me.recipientUserId, Long.MinValue) > (me.ts - 7.days)
          case None => false
        }
        if (isResponse) {
          messageSentInResponseFraction.merge(me.ts, me.senderUserId, frac => (frac._1 + 1, frac._2 + 1))
        } else {
          messageSentInResponseFraction.merge(me.ts, me.senderUserId, frac => (frac._1, frac._2 + 1))
        }
    }
    // RULE 5 EVALUATION
    bind {
      me: MessageEvent =>
        messageSentInResponseFraction.get(me.ts, me.senderUserId) match {
          case Some((resp, all)) =>
            if ((resp + 0.0) / all < MessageSentInResponseFractionThreshold) {
              messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
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
