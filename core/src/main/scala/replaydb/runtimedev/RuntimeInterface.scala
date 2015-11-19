package replaydb.runtimedev

import replaydb.event.Event
import replaydb.runtimedev.threadedImpl.RunProgressCoordinator


trait RuntimeInterface {
  def numPhases: Int
  def update(partitionId: Int, phase: Int, e: Event): Unit
  def update(partitionId: Int, e: Event): Unit = {
    for (i <- 0 until numPhases) {
      update(partitionId, i, e)
    }
  }

  var runProgressCoordinator: RunProgressCoordinator = null
  def setRunProgressCoordinator(rpc: RunProgressCoordinator): Unit = {
    runProgressCoordinator = rpc
  }
  def getDeltaFromState(state: ReplayState, partitionId: Int, phaseId: Int): ReplayDelta = {
    runProgressCoordinator.getDeltaForState(state, partitionId, phaseId)
  }
}
