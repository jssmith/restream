package replaydb.runtimedev

import replaydb.event.Event


trait DistributedRuntimeInterface {
  def numPhases: Int
  def update(partitionId: Int, phase: Int, e: Event): Unit
  def update(partitionId: Int, e: Event): Unit = {
    for (i <- 0 until numPhases) {
      update(partitionId, i, e)
    }
  }
}
