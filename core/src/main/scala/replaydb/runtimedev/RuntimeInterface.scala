package replaydb.runtimedev

import replaydb.event.Event

trait RuntimeInterface {
  def numPhases: Int
  def update(phase: Int, e: Event)(implicit batchInfo: BatchInfo): Unit
  def update(e: Event)(implicit batchInfo: BatchInfo): Unit = {
    update(batchInfo.phaseId, e)(batchInfo)
  }
  def updateAllPhases(e: Event)(implicit batchInfo: BatchInfo): Unit = {
    for (i <- 0 until numPhases) {
      update(i, e)(batchInfo)
    }
  }
}
