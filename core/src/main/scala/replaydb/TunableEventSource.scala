package replaydb


import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.util.{FastMath, MathArrays}
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

import scala.collection.immutable.HashMap

class TunableEventSource (startTime: Long, numUsers: Int, rnd: RandomGenerator,
                          words: Array[String], alpha: Double = 2.0, spammerFraction: Double = 0.02) extends EventSource {
  private var t = startTime
  private var messageIdSeq = 0L
  private val (userZipfDistribution, userProbabilities) = {
    val users = (1 to numUsers).toArray
    val zipfAlpha = 1D / (alpha - 1)
    val x = users.map(1.0 / FastMath.pow(_, zipfAlpha))
    val xs = x.sum
    val probabilities = x.map(_/xs)
    MathArrays.shuffle(users, rnd)
    println(s"number of users generated is $numUsers | ${probabilities.length}")
    (new EnumeratedIntegerDistribution(rnd, users, probabilities),
      HashMap() ++ (users.map(_.toLong) zip probabilities))
  }
  private val (userIpAddresses, spammerIpAddresses) = {
    val numIps = numUsers / 10
    val numSpammerIps = Math.max(numIps, (numIps * spammerFraction * 5).toInt)
    val ips = (1 to numIps).toArray
    MathArrays.shuffle(ips, rnd)
    val spammerIps = ips.take(numSpammerIps)
    (ips, spammerIps)
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

  def isValid(userId: Long): Boolean = {
    userId > 0 && userId <= numUsers
  }

  private def getIpAddress(senderUserId: Long, spammer: Boolean): Int = {
    if (spammer) {
      spammerIpAddresses((senderUserId % spammerIpAddresses.length).toInt)
    } else {
      userIpAddresses((senderUserId % userIpAddresses.length).toInt)
    }
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
        val m = 10
        val d = Math.max(2, (userProbabilities(userIdA) * m * numUsers).toInt)
        val userIdB = (userIdA + (if (rnd.nextBoolean()) {
          rnd.nextInt(d/2) + 1
        } else {
          -(rnd.nextInt(d/2) + 1)
        }) + m * numUsers - 1) % numUsers + 1
        val userPair = (userIdA, userIdB)
        val e = if (!bf.mightContain(userPair) && rnd.nextBoolean()) {
          bf.put(userPair)
          new NewFriendshipEvent(t, userIdA, userIdB)
        } else {
          new MessageEvent(t, nextMessageId(), userIdA, userIdB, getIpAddress(userIdA, false), getRandomMessage(false))
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
          f(new MessageEvent(t, nextMessageId(), userIdA, userIdB, getIpAddress(userIdA, true), getRandomMessage(true)))
          t += rnd.nextInt(10000)
          i += 1
        }
      }
      t += rnd.nextInt(100000)
    }
  }
}
