package replaydb.exec

import java.io.FileInputStream

import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev.threadedImpl.{ReplayCounterImpl, ReplayMapImpl, RunProgressCoordinator}
import replaydb.runtimedev.{ReplayCounter, ReplayMap, _}
import replaydb.util.ProgressMeter

object ParallelSpamDetector extends App {

  class UserPair(val a: Long, val b: Long) {
    override def equals(o: Any) = o match {
      case that: UserPair => (a == that.a && b == that.b) || (a == that.b && b == that.a)
      case _ => false
    }
    override def hashCode(): Int = {
      a.hashCode() + 25741 * b.hashCode()
    }
  }

  case class PrintSpamCounter(ts: Long) extends Event

  class Stats {
    val friendships: ReplayMap[UserPair, Int] = new ReplayMapImpl[UserPair, Int](0)
    val friendSendRatio: ReplayMap[Long, (Long, Long)] =
      new ReplayMapImpl[Long, (Long, Long)]((0L,0L))
    val spamCounter: ReplayCounter = new ReplayCounterImpl
    def getRuntimeInterface: RuntimeInterface = emit {
      bind { e: NewFriendshipEvent =>
        friendships.update(ts = e.ts, key = new UserPair(e.userIdA, e.userIdB), fn = _ => 1)
      }
      bind { me: MessageEvent =>
        val ts = me.ts
        friendships.get(ts = ts, key = new UserPair(me.senderUserId, me.recipientUserId)) match {
          case Some(_) => friendSendRatio.update(ts = ts, key = me.senderUserId, {case (friends, nonFriends) => (friends + 1, nonFriends)})
          case None => friendSendRatio.update(ts = ts, key = me.senderUserId, {case (friends, nonFriends) => (friends, nonFriends + 1)})
        }
      }
      bind {
        me: MessageEvent =>
          val ts = me.ts
          friendSendRatio.get(ts = ts, key = me.senderUserId) match {
            case Some((toFriends, toNonFriends)) =>
              if (toFriends + toNonFriends > 5) {
                if (toNonFriends > toFriends) {
                  spamCounter.add(1, ts)
                }
              }
            case None =>
          }
      }
      bind {
        e: PrintSpamCounter => println(s"spam count is ${spamCounter.get(e.ts)}")
      }
    }
  }

  val numPartitions = 4
  val partitionFnBase = "/tmp/events.out"
  val stats = new Stats
  val si = stats.getRuntimeInterface
  val numPhases = si.numPhases
  var lastTimestamp = 0L
  val barrier = new RunProgressCoordinator(numPartitions = numPartitions, numPhases = numPhases, maxEventsPerPhase = 20000)
  val overallProgressMeter = new ProgressMeter(1000000, name = Some("Overall Progress"))
  val xInterval = 1000
  val threads =
    for (partitionId <- 0 until numPartitions; phaseId <- 0 until si.numPhases) yield {
      new Thread(new Runnable {
        val b = barrier.getCoordinatorInterface(partitionId, phaseId)
        override def run(): Unit = {
          val pm = new ProgressMeter(printInterval = 1000000, () => s"${MemoryStats.getStats()}", name = Some(s"$partitionId-$phaseId"))
          var ct = 0L
          var limitTs = b.requestProgress(0, ct)
          val eventStorage = new SocialNetworkStorage
          eventStorage.readEvents(new FileInputStream(s"$partitionFnBase-$partitionId"), e => {
            while (e.ts > limitTs) {
              limitTs = b.requestProgress(e.ts, ct)
            }
            si.update(phaseId, e)
            ct += 1
            if (ct % xInterval == 0) {
              if (phaseId == numPhases - 1) {
                overallProgressMeter.synchronized { overallProgressMeter.add(xInterval) }
              }
              b.update(e.ts - 1, ct)
            }
            lastTimestamp = e.ts
            pm.increment()
          })
          overallProgressMeter.synchronized { overallProgressMeter.add((ct % xInterval).toInt) }
          b.finished()
          pm.finished()
        }
      }, s"process-events-$partitionId-$phaseId")
    }
  threads.foreach(_.start())
  threads.foreach(_.join())
  overallProgressMeter.synchronized{ overallProgressMeter.finished() }
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue))
}
