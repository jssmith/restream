package replaydb

import org.apache.commons.math3.random.RandomGenerator
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

/**
 *
 * @param startTime
 * @param numUsers
 * @param rnd
 */
class UniformEventSource (startTime: Long, numUsers: Long, rnd: RandomGenerator) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L

  private def nextMessageId(): Long = {
    messageIdSeq += 1
    messageIdSeq
  }

  override def genEvents(n: Int, f: Event=>Unit) {
    for (i <- 0 until n) {
      val userIdA = (rnd.nextLong() & Long.MaxValue) % numUsers + 1
      val userIdB = (rnd.nextLong() & Long.MaxValue) % numUsers + 1
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
