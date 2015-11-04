package replaydb


import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.util.FastMath
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.HashMap
import scala.util.Random

class TunableEventSource (startTime: Long, numUsers: Int, rnd: RandomGenerator, alpha: Double = 1.0, spammerFraction: Double = 0.1) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L
  private val (userZipfDistribution, userProbabilities) = {
    val users = (1 to numUsers).toArray
    val x = users.map(1.0 / FastMath.pow(_, alpha))
    val xs = x.sum
    val probabilities = x.map(_/xs)
    val usersShuffled = Random.shuffle(users.toSeq).toArray
    (new EnumeratedIntegerDistribution(rnd, usersShuffled, probabilities),
      HashMap() ++ (usersShuffled.map(_.toLong) zip probabilities))
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

  private def nextUserZipf(): Long = {
    userZipfDistribution.sample
  }

  private def nextUserUniform(): Long = {
    rnd.nextInt(numUsers) + 1
  }

  override def genEvents(n: Int, f: (Event) => Unit): Unit = {
    val spammerThreshold = (spammerFraction * numUsers).toInt
    for (i <- 0 until n) {
      val userIdA = nextUserZipf()
      val e = if (userIdA > spammerThreshold) {
        // Normal users choose their targets according to Zipfian distribution
        val d = Math.max(2, (userProbabilities(userIdA) * 10 * numUsers).toInt)
        val userIdB = (userIdA + (if (rnd.nextBoolean()) {
          rnd.nextInt(d/2) + 1
        } else {
          -(rnd.nextInt(d/2) + 1)
        }) + numUsers - 1) % numUsers + 1
        val userPair = (userIdA, userIdB)
        if (!bf.mightContain(userPair) && rnd.nextBoolean()) {
          bf.put(userPair)
          f(new NewFriendshipEvent(t, userIdB, userIdA)) // TODO a little hacky..
          new NewFriendshipEvent(t, userIdA, userIdB)
        } else {
          new MessageEvent(t, nextMessageId(), userIdA, userIdB, "")
        }
      } else {
        // Spammers choose their targets at random
        var userIdB = userIdA
        while (userIdA == userIdB) {
          userIdB = nextUserUniform()
        }
        new MessageEvent(t, nextMessageId(), userIdA, userIdB, "")
      }
      f(e)
      t += rnd.nextInt(100000)
    }
  }
}
