package replaydb

import replaydb.event.{MessageEvent, NewFriendshipEvent, Event}

import scala.util.Random

class EventSource(startTime: Long, numUsers: Long, rnd: Random) {
  private var t = startTime
  private var messageIdSeq = 0L

  private def nextMessageId(): Long = {
    messageIdSeq += 1
    messageIdSeq
  }

  def genEvents(n: Int, f: Event=>Unit) {
    for (i <- 0 to n) {
      val userIdA = (rnd.nextLong() & 0x7fffffffffffffffL) % numUsers + 1
      val userIdB = (rnd.nextLong() & 0x7fffffffffffffffL) % numUsers + 1
      // For now we generate new friendships and messages with equal probability
      val e = if (rnd.nextBoolean()) {
        new NewFriendshipEvent(t, userIdA, userIdB)
      } else {
        new MessageEvent(t, nextMessageId(), userIdA, userIdB, "")
      }
      f(e)
      t += rnd.nextInt(100000)
    }
  }

}
