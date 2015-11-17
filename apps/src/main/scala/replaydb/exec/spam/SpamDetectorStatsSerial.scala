package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.monotonicImpl.{ReplayCounterImpl, ReplayMapImpl, ReplayTimestampLocalMapImpl}
import replaydb.util.time._
import replaydb.event.Event

import scala.collection.immutable

class SpamDetectorStatsSerial {

  val friendships: ReplayMap[UserPair, Int] = new ReplayMapImpl(0)
  val friendSendRatio: ReplayMap[Long, (Long, Long)] = new ReplayMapImpl((0L,0L))
  val spamCounter: ReplayCounter = new ReplayCounterImpl
  val nonfriendMessageTimestamps: ReplayMap[Long, Seq[Long]] = new ReplayMapImpl(Seq[Long]())
  // Mapping userIDa -> (userIDb -> # messages sent userIDa to userIDb in last 5 minutes)
  val uniqueNonfriendsSentTo: ReplayMap[Long, immutable.Map[Long, Long]] = new ReplayMapImpl(new immutable.HashMap)
  val messageContainingEmailFraction: ReplayMap[Long, (Seq[Long], Seq[Long])] =
    new ReplayMapImpl((Seq[Long](), Seq[Long]()))
  val messageSpamRatings: ReplayTimestampLocalMap[Long, Int] = new ReplayTimestampLocalMapImpl(0)

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState] = {
    List(friendships, friendSendRatio, spamCounter, messageSpamRatings, uniqueNonfriendsSentTo, nonfriendMessageTimestamps)
  }

  val SpamScoreThreshold = 10

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { e: NewFriendshipEvent =>
      friendships.update(ts = e.ts, key = new UserPair(e.userIdA, e.userIdB), fn = _ => 1)
    }
    // RULE 1 STATE
    bind { me: MessageEvent =>
      val ts = me.ts
      friendships.get(ts = ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
        case Some(_) => friendSendRatio.update(ts = ts, key = me.senderUserId, {case (friends, nonFriends) => (friends + 1, nonFriends)})
        case None => friendSendRatio.update(ts = ts, key = me.senderUserId, {case (friends, nonFriends) => (friends, nonFriends + 1)})
      }
    }
    // RULE 1 EVALUATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        friendSendRatio.get(ts = ts, key = me.senderUserId) match {
          case Some((toFriends, toNonFriends)) =>
            if (toFriends + toNonFriends > 5) {
              if (toNonFriends > 2 * toFriends) {
                //                  spamCounter.add(1, ts)
                messageSpamRatings.update(ts, me.messageId, _ + 10)
              }
            }
          case None =>
        }
    }
    // RULE 2 STATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        friendships.get(ts = ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
          case Some(_) =>
          case None =>
            nonfriendMessageTimestamps.update(ts, me.senderUserId, seq => {
              (seq :+ me.ts).filter(_ > (me.ts - NonfriendMessageCountInterval))
            })
            uniqueNonfriendsSentTo.update(ts, me.senderUserId,
              map => map.updated(me.recipientUserId, me.ts))
        }
    }
    // RULE 2 EVALUATE
    bind {
      // if sent more than UniqueNonfriendCountSpamThreshold messages to > NonfriendMessageCountSpamThreshold
      // distinct nonfriends in last NonfriendMessageCountInterval, spam
      me: MessageEvent =>
        val msgCnt = nonfriendMessageTimestamps.get(me.ts, me.senderUserId) match {
          case Some(tsSeq) => tsSeq.count(_ > (me.ts - NonfriendMessageCountInterval))
          case None => 0
        }
        if (msgCnt > NonfriendMessageCountSpamThreshold) {
          val nonfriendCnt = uniqueNonfriendsSentTo.get(me.ts, me.senderUserId) match {
            case Some(friendMap) => friendMap.count(_._2 > (me.ts - NonfriendMessageCountInterval))
            case None => 0
          }
          if (nonfriendCnt > UniqueNonfriendCountSpamThreshold) {
            messageSpamRatings.update(me.ts, me.messageId, _ + 10)
          }
        }
    }
    // RULE 3 STATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        if (containsEmail(me.content)) {
          messageContainingEmailFraction.update(ts, me.senderUserId,
            (ratio) => ((ratio._1 :+ ts).filter(_ > ts - MessageContainingEmailInterval),
              ratio._2.filter(_ > ts - MessageContainingEmailInterval)))
        } else {
          messageContainingEmailFraction.update(ts, me.senderUserId,
            (ratio) => (ratio._1.filter(_ > ts - MessageContainingEmailInterval),
              (ratio._2 :+ ts).filter(_ > ts - MessageContainingEmailInterval)))
        }
    }
    // RULE 3 EVALUATE
    bind {
      me: MessageEvent =>
        val ts = me.ts
        val fraction = messageContainingEmailFraction.get(ts, me.senderUserId) match {
          case Some((emails: Seq[Long], nonemails: Seq[Long])) =>
            val emailCount: Double = emails.count(_ > ts - MessageContainingEmailInterval)
            val allCount: Double = nonemails.count(_ > ts - MessageContainingEmailInterval) + emailCount
            if (emailCount / allCount > MessageContainingEmailFractionThreshold) {
              messageSpamRatings.update(ts, me.messageId, _ + 10)
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
