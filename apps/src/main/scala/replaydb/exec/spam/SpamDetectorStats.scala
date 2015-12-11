package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._
import replaydb.util.time._
/*
RULES:

1. spam if sent messages to non friends > 2 * (messages to friends) and total messages > 5
2. spam if you've sent > 7 messages to nonfriends in last 120 minutes with at least 5 unique recipients
3. spam if > 0.5 of your messages in the last 30 days contain an email address
4. spam if > 0.2 of your messages from the last 7 days were sent in the last day (and your first message was >= 7 days ago)
5. spam if < 0.1 of your messages for all time were sent as responses (defined as
  //         a message A->B when a message B->A exists within the last 7 days)

 */

class SpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends RuntimeStats with HasSpamCounter {
  import replayStateFactory._

  val friendships: ReplayMap[UserPair, Int] = getReplayMap(0)
  val friendSendRatio: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
//  val spamCounter: ReplayCounter = getReplayCounter
  val spamCounter: ReplayCounter = getReplayCounter
  val nonfriendMessagesInLastInterval: ReplayMap[Long, Long] = getReplayMap(0)
  // Mapping userIDa -> (userIDb -> # messages sent userIDa to userIDb in last NonfriendMessageCountInterval)
//  val uniqueNonfriendsSentToInLastInterval: ReplayMap[Long, immutable.Map[Long, Long]] =
//    getReplayMap(new immutable.HashMap)
  val messageContainingEmailFraction: ReplayMap[Long, (Long, Long)] = getReplayMap((0L, 0L))
  val messagesFractionLast7DaysInLast24Hours: ReplayMap[Long, (Long, Long)] = getReplayMap((0L,0L))
  val userFirstMessageTS: ReplayMap[Long, Long] = getReplayMap(Long.MaxValue)
  val userMostRecentReceivedMessage: ReplayMap[(Long, Long), Long] = getReplayMap(Long.MinValue)
  val messageSentInResponseFraction: ReplayMap[Long, (Long, Long)] = getReplayMap((0L, 0L))
  val messageSpamRatings: ReplayTimestampLocalMap[Long, Int] = getReplayTimestampLocalMap(0)

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(friendships, friendSendRatio, spamCounter, messageSpamRatings, //uniqueNonfriendsSentToInLastInterval,
      nonfriendMessagesInLastInterval, messageContainingEmailFraction, messagesFractionLast7DaysInLast24Hours,
      userFirstMessageTS, userMostRecentReceivedMessage, messageSentInResponseFraction)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  val toOne = (_: Int) => 1
  val incrBy10 = (x: Int) => x + 10
  val incrFirst = (tuple: (Long, Long)) => (tuple._1 + 1, tuple._2)
  val incrSecond = (tuple: (Long, Long)) => (tuple._1, tuple._2 + 1)
  val incrBoth = (tuple: (Long, Long)) => (tuple._1 + 1, tuple._2 + 1)
  val decrBoth = (tuple: (Long, Long)) => (tuple._1 - 1, tuple._2 - 1)
  val decrFirst = (tuple: (Long, Long)) => (tuple._1 - 1, tuple._2)
  val decrSecond = (tuple: (Long, Long)) => (tuple._1, tuple._2 - 1)
  val takeMax = (newVal: Long) => (old: Long) => Math.max(old, newVal)
  val takeMin = (newVal: Long) => (old: Long) => Math.min(old, newVal)

  registerClass(toOne.getClass)
  registerClass(incrFirst.getClass)
  registerClass(incrSecond.getClass)
  registerClass(incrBy10.getClass)
  registerClass(incrBoth.getClass)
  registerClass(decrBoth.getClass)
  registerClass(decrFirst.getClass)
  registerClass(decrSecond.getClass)
  registerClass(takeMax(0).getClass)
  registerClass(takeMin(0).getClass)

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { e: NewFriendshipEvent =>
      friendships.merge(ts = e.ts, key = new UserPair(e.userIdA, e.userIdB), fn = toOne)
    }
    // RULE 1 STATE
    bind { me: MessageEvent =>
      friendships.get(ts = me.ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
        case Some(_) => friendSendRatio.merge(ts = me.ts, key = me.senderUserId, incrFirst)
        case None => friendSendRatio.merge(ts = me.ts, key = me.senderUserId, incrSecond)
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
                messageSpamRatings.merge(me.ts, me.messageId, incrBy10)
              }
            }
          case None =>
        }
    }
    // RULE 2 STATE
    bind {
      me: MessageEvent =>
        friendSendRatio.get(ts = me.ts, key = me.senderUserId) match {
          case Some((toFriends, toNonFriends)) =>
            if (toFriends + toNonFriends > 5) {
              if (toNonFriends > 2 * toFriends) {
                //                  spamCounter.add(1, ts)
                messageSpamRatings.merge(me.ts, me.messageId, incrBy10)
              }
            }
          case None =>
        }
    }
//    // RULE 2 STATE
//    bind {
//      me: MessageEvent =>
//        friendships.get(ts = me.ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
//          case Some(_) =>
//          case None =>
//            nonfriendMessagesInLastInterval.merge(me.ts, me.senderUserId, _ + 1)
//            nonfriendMessagesInLastInterval.merge(me.ts + NonfriendMessageCountInterval, me.senderUserId, _ - 1)
//            uniqueNonfriendsSentToInLastInterval.merge(me.ts, (me.senderUserId, me.recipientUserId),
//              map => map.updated(me.recipientUserId, map.getOrElse(me.recipientUserId, 0L) + 1))
//            uniqueNonfriendsSentToInLastInterval.merge(me.ts + NonfriendMessageCountInterval, me.senderUserId,
//              map => { val x = map.getOrElse(me.recipientUserId, 0L); if (x > 0) map.updated(me.recipientUserId, x - 1) else null })
//              // null case should never happen - leave null to throw NPE
//        }
//    }
//    // RULE 2 EVALUATION
//    bind {
//      // if sent more than NonfriendMessageCountSpamThreshold messages to > UniqueNonfriendCountSpamThreshold
//      // distinct nonfriends in last NonfriendMessageCountInterval, spam
//      // NOTE: these numbers should probably be higher (and the time period shorter) but our current
//      //       generator doesn't actually create any messages matching this if I turn them up
//      me: MessageEvent =>
//        val msgCnt = nonfriendMessagesInLastInterval.get(me.ts, me.senderUserId) match {
//          case Some(cnt) => cnt
//          case None => 0
//        }
//        if (msgCnt > NonfriendMessageCountSpamThreshold) {
//          val nonfriendCnt = uniqueNonfriendsSentToInLastInterval.get(me.ts, me.senderUserId) match {
//            case Some(friendMap) => friendMap.count(_._2 > 0)
//            case None => 0
//          }
//          if (nonfriendCnt > UniqueNonfriendCountSpamThreshold) {
//            messageSpamRatings.merge(me.ts, me.messageId, _ + 10)
//          }
//        }
//    }
    // RULE 3 STATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        if (containsEmail(me.content)) {
          messageContainingEmailFraction.merge(ts, me.senderUserId, incrBoth)
          messageContainingEmailFraction.merge(ts + MessageContainingEmailInterval, me.senderUserId, decrBoth)
        } else {
          messageContainingEmailFraction.merge(ts, me.senderUserId, incrSecond)
          messageContainingEmailFraction.merge(ts + MessageContainingEmailInterval, me.senderUserId, decrSecond)
        }
    }
    // RULE 3 EVALUATION
    bind {
      me: MessageEvent =>
        messageContainingEmailFraction.get(me.ts, me.senderUserId) match {
          case Some((email: Long, all: Long)) =>
            if ((email + 0.0) / all > MessageContainingEmailFractionThreshold) {
              messageSpamRatings.merge(me.ts, me.messageId, incrBy10)
            }
          case None =>
        }
    }
    // RULE 4 STATE
    bind {
      me: MessageEvent =>
        userFirstMessageTS.merge(me.ts, me.senderUserId, takeMin(me.ts)) // kind of wasteful...
        messagesFractionLast7DaysInLast24Hours.merge(me.ts, me.senderUserId, incrBoth)
        messagesFractionLast7DaysInLast24Hours.merge(me.ts + 1.days, me.senderUserId, decrFirst)
        messagesFractionLast7DaysInLast24Hours.merge(me.ts + 7.days, me.senderUserId, decrSecond)
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
                messageSpamRatings.merge(me.ts, me.messageId, incrBy10)
              }
            case None =>
          }
        }
    }
    // RULE 5 STATE 1
    bind {
      me: MessageEvent =>
        userMostRecentReceivedMessage.merge(me.ts, (me.recipientUserId, me.senderUserId), takeMax(me.ts))
    }
    // RULE 5 STATE 2
    bind {
      me: MessageEvent =>
        val isResponse = userMostRecentReceivedMessage.get(me.ts, (me.senderUserId, me.recipientUserId)) match {
          case Some(v) => v > (me.ts - 7.days)
          case None => false
        }
        if (isResponse) {
          messageSentInResponseFraction.merge(me.ts, me.senderUserId, incrBoth)
        } else {
          messageSentInResponseFraction.merge(me.ts, me.senderUserId, incrSecond)
        }
    }
    // RULE 5 EVALUATION
    bind {
      me: MessageEvent =>
        messageSentInResponseFraction.get(me.ts, me.senderUserId) match {
          case Some((resp, all)) =>
            if ((resp + 0.0) / all < MessageSentInResponseFractionThreshold) {
              messageSpamRatings.merge(me.ts, me.messageId, incrBy10)
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
