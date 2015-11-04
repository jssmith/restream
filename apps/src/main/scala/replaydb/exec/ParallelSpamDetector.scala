package replaydb.exec

import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev.threadedImpl.{MultiReaderEventSource, ReplayCounterImpl, ReplayMapImpl, RunProgressCoordinator}
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
//      bind {
//        e: PrintSpamCounter => println(s"spam count is ${spamCounter.get(e.ts)}")
//      }
    }
  }

  val numPartitions = 4
  val partitionFnBase = "/tmp/events.out"

  val coordinationInterval = if (args.length >= 1) { args(0).toInt } else { 1000 }
  val maxInProgressEvents = if (args.length >= 2) { args(1).toInt } else { 20000 }
  println(s"coordination interval = $coordinationInterval")
  println(s"max in progress events = $maxInProgressEvents")

  val stats = new Stats
  val si = stats.getRuntimeInterface
  val numPhases = si.numPhases
  println("num phases: " + numPhases)
  var lastTimestamp = 0L
  val barrier = new RunProgressCoordinator(numPartitions = numPartitions, numPhases = numPhases, maxInProgressEvents = maxInProgressEvents)
  val overallProgressMeter = new ProgressMeter(1000000, name = Some("Overall Progress"))
  val readerThreads = (for (partitionId <- 0 until numPartitions) yield {
    new MultiReaderEventSource(s"$partitionFnBase-$partitionId", numPhases, bufferSize = 100000)
  }).toArray
  val threads =
    for (partitionId <- 0 until numPartitions; phaseId <- 0 until si.numPhases) yield {
      new Thread(new Runnable {
        val b = barrier.getCoordinatorInterface(partitionId, phaseId)
        override def run(): Unit = {
          val pm = new ProgressMeter(printInterval = 1000000, () => s"${MemoryStats.getStats()}", name = Some(s"$partitionId-$phaseId"))
          var ct = 0L
          var limitTs = b.requestProgress(0, ct)
          readerThreads(partitionId).readEvents(e => {
            while (e.ts > limitTs) {
              limitTs = b.requestProgress(e.ts, ct)
            }
            si.update(phaseId, e)
            ct += 1
            if (ct % coordinationInterval == 0) {
              if (phaseId == numPhases - 1) {
                overallProgressMeter.synchronized { overallProgressMeter.add(coordinationInterval) }
              }
              b.update(e.ts - 1, ct)
            }
            lastTimestamp = e.ts
            pm.increment()
          })
          if (phaseId == numPhases - 1) {
            overallProgressMeter.synchronized { overallProgressMeter.add((ct % coordinationInterval).toInt) }
          }
          b.finished()
          pm.finished()
        }
      }, s"process-events-$partitionId-$phaseId")
    }
  readerThreads.foreach(_.start())
  threads.foreach(_.start())
  readerThreads.foreach(_.join())
  threads.foreach(_.join())
  overallProgressMeter.synchronized{ overallProgressMeter.finished() }
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue))
}
