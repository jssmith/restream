package replaydb


import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.util.{FastMath, MathArrays}
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.HashMap

class TunableEventSource (startTime: Long, numUsers: Int, rnd: RandomGenerator,
                          words: Array[String], alpha: Double = 1.0, spammerFraction: Double = 0.02) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L
  private val (userZipfDistribution, userProbabilities) = {
    val users = (1 to numUsers).toArray
    val x = users.map(1.0 / FastMath.pow(_, alpha))
    val xs = x.sum
    val probabilities = x.map(_/xs)
    MathArrays.shuffle(users, rnd)
    (new EnumeratedIntegerDistribution(rnd, users, probabilities),
      HashMap() ++ (users.map(_.toLong) zip probabilities))
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

  // 50% of spammer messages contain emails,
  // 10% of normal messages contain emails
  private def getRandomMessage(spammer: Boolean): String = {
    val messageLength = 5 + rnd.nextInt(15)
    (for (_ <- 0 until messageLength) yield {
      getRandomWord
    }).mkString(" ") +
      (if (spammer && rnd.nextBoolean() || (!spammer && rnd.nextInt(10) < 1))
        s" $getRandomWord@email.com "
      else "")
  }

  private def getRandomWord: String = {
    words(rnd.nextInt(words.length))
  }

  override def genEvents(n: Int, f: (Event) => Unit): Unit = {
    val spammerThreshold = (spammerFraction * numUsers).toInt
    var i = 0
    while (i < n) {
      val userIdA = nextUserZipf()
      if (userIdA > spammerThreshold) {
        // Normal users choose their targets according to Zipfian distribution
        val d = Math.max(2, (userProbabilities(userIdA) * 10 * numUsers).toInt)
        val userIdB = (userIdA + (if (rnd.nextBoolean()) {
          rnd.nextInt(d/2) + 1
        } else {
          -(rnd.nextInt(d/2) + 1)
        }) + numUsers - 1) % numUsers + 1
        val userPair = (userIdA, userIdB)
        val e = if (!bf.mightContain(userPair) && rnd.nextBoolean()) {
          bf.put(userPair)
          new NewFriendshipEvent(t, userIdA, userIdB)
        } else {
          new MessageEvent(t, nextMessageId(), userIdA, userIdB, getRandomMessage(false))
        }
        f(e)
        i += 1
      } else {
        // Spammers choose their targets at random and fire in bursts
        for (_ <- 0 until 20) {
          var userIdB = userIdA
          while (userIdA == userIdB) {
            userIdB = nextUserUniform()
          }
          f(new MessageEvent(t, nextMessageId(), userIdA, userIdB, getRandomMessage(true)))
          t += rnd.nextInt(10000)
          i += 1
        }
      }
      t += rnd.nextInt(100000)
    }
  }
}
