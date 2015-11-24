package replaydb.runtimedev

import replaydb.event.Event


trait RuntimeInterface {
  def numPhases: Int
  def update(partitionId: Int, phase: Int, e: Event, deltaMap: Map[ReplayState, ReplayDelta]): Unit
  def update(partitionId: Int, e: Event, deltaMap: Map[ReplayState, ReplayDelta]): Unit = {
    for (i <- 1 to numPhases) {
      update(partitionId, i, e, deltaMap)
    }
  }
}
