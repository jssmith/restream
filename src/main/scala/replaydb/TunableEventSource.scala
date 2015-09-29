package replaydb

import com.google.common.hash.{PrimitiveSink, Funnel, BloomFilter}
import org.apache.commons.math3.random.{RandomDataGenerator, RandomGenerator}
import replaydb.event.{NewFriendshipEvent, MessageEvent, Event}

class TunableEventSource (startTime: Long, numUsers: Int, rnd: RandomGenerator, alpha: Double = 1.0) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L
  private val userGenerator = new RandomDataGenerator(rnd)
  private val bf = BloomFilter.create(new Funnel[(Long,Long)] {
    override def funnel(from: (Long, Long), into: PrimitiveSink): Unit = {
      if (from._1 < from._2) {
        into.putLong(from._1).putLong(from._2)
      } else {
        into.putLong(from._2).putLong(from._1)
      }
    }
  }, Math.max(numUsers * 1000, 100000)) // TODO - need to properly size the bloom filter...

  private def nextMessageId(): Long = {
    messageIdSeq += 1
    messageIdSeq
  }

  private def nextUser(): Long = {
    // TODO this slows down considerably when as the number of users increases
    userGenerator.nextZipf(numUsers, alpha) + 1
  }

  override def genEvents(n: Int, f: (Event) => Unit): Unit = {
    for (i <- 0 until n) {
      val userIdA = nextUser()
      var userIdB = nextUser()
      while (userIdA == userIdB) {
        userIdB = nextUser()
      }
      val userPair = (userIdA,userIdB)
      val e = if (!bf.mightContain(userPair) && rnd.nextBoolean()) {
        bf.put(userPair)
        new NewFriendshipEvent(t, userIdA, userIdB)
      } else {
        new MessageEvent(t, nextMessageId(), userIdA, userIdB, "")
      }
      f(e)
      t += rnd.nextInt(100000)
    }
  }
}
