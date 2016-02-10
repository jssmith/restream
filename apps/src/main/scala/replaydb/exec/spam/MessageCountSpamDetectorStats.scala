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
class MessageCountSpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {
  import replayStateFactory._

  val messageSendCounts: ReplayMap[Long, Int] = getReplayMap(0)

  val spamCounter: ReplayCounter = new ReplayCounter {
    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = 0L

    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = { }

    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = { }
  }

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

  def getRuntimeInterface: RuntimeInterface = emit {
    bind {
      me: MessageEvent => messageSendCounts.merge(ts = me.ts, key = me.senderUserId, ct => ct + 1)
    }
  }
}
