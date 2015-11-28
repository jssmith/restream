package replaydb.runtimedev

import replaydb.event.Event


trait DistributedRuntimeInterface {
  def numPhases: Int
  def update(partitionId: Int, phase: Int, e: Event, deltaMap: Map[ReplayState, ReplayState]): Unit
  def update(partitionId: Int, e: Event, deltaMap: Map[ReplayState, ReplayState]): Unit = {
    for (i <- 1 to numPhases) {
      update(partitionId, i, e, deltaMap)
    }
  }
}
