package replaydb.runtimedev

import replaydb.event.Event
import replaydb.runtimedev.threadedImpl.RunProgressCoordinator


trait RuntimeInterface {
  def numPhases: Int
  def update(phase: Int, e: Event): Unit
  def update(e: Event): Unit = {
    for (i <- 0 until numPhases) {
      update(i, e)
    }
  }

  var runProgressCoordinator: RunProgressCoordinator = null
  def setRunProgressCoordinator(rpc: RunProgressCoordinator): Unit = {
    runProgressCoordinator = rpc
  }
  def getDeltaFromState(state: ReplayState, phaseId: Int): ReplayDelta = {
    runProgressCoordinator.getDeltaForState(state, phaseId)
  }
}
