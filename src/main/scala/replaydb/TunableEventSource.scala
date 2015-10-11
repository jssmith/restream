package replaydb


import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.commons.math3.distribution.{EnumeratedIntegerDistribution}
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.util.FastMath
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.util.Random

class TunableEventSource (startTime: Long, numUsers: Int, rnd: RandomGenerator, alpha: Double = 1.0) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L
  private val userZipfDistribution = {
    val users = (1 to numUsers).toArray
    val x = users.map(1.0 / FastMath.pow(_, alpha))
    val xs = x.sum
    val probabilities = users.map(_/xs)
    new EnumeratedIntegerDistribution(rnd, Random.shuffle(users.toSeq).toArray, probabilities)
  }

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
    userZipfDistribution.sample
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
