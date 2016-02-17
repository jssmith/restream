package replaydb.exec.spam

import replaydb.event.{MessageEvent, Event}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._



class DoNothingSpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {

  // TODO should be able to clean up here...
  val spamCounter: ReplayCounter = new ReplayCounter {
    override def increment(ts: Long)(implicit batchInfo: BatchInfo): Unit = { }

    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = 0L

    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = { }

    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = { }
  }

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(spamCounter)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  def getRuntimeInterface: RuntimeInterface = emit {
    bind {
      me: MessageEvent =>
    }
  }
}
