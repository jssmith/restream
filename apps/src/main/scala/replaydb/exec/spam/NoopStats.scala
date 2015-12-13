package replaydb.exec.spam

import replaydb.event.Event
import replaydb.runtimedev._
import replaydb.runtimedev.ReplayRuntime._

class NoopStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory)
  extends HasRuntimeInterface with HasReplayStates[ReplayState] with HasSpamCounter {

  val spamCounter = replayStateFactory.getReplayCounter

  override def getAllReplayStates: Seq[ReplayState] = Seq()

  override def getRuntimeInterface: RuntimeInterface = emit {
    bind {
      e: Event => 1 
    }
  }

}
