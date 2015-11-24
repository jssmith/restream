package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayState

trait ReplayDelta extends ReplayState {

  def clear(): Unit

}
