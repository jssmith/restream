package replaydb.exec.spam

import replaydb.event.MessageEvent
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._


/**
 * Counts the number of messages sent by each user
 *
 * @param replayStateFactory
 */
class MessageCountThresholdSpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {
  import replayStateFactory._

  val messageSendCounts: ReplayMap[Long, Int] = getReplayMap(0)

  val spamCounter: ReplayCounter = getReplayCounter

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(messageSendCounts, spamCounter)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  def incrementInt(i: Int): Int = i + 1

  def getRuntimeInterface: RuntimeInterface = emit {
    bind {
      me: MessageEvent => messageSendCounts.merge(ts = me.ts, key = me.senderUserId, incrementInt)
    }
    bind {
      me: MessageEvent =>
        messageSendCounts.get(ts = me.ts, key = me.senderUserId) match {
          case Some(9) => spamCounter.increment(me.ts)
          case _ =>
        }
    }
  }
}
