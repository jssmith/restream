package replaydb.service

import replaydb.service.driver.RunConfiguration

class ProgressTracker(runConfiguration: RunConfiguration) {
  import runConfiguration._

  private val positions = Array.ofDim[Long](numPhases,numPartitions)
  private def batchId(ts: Long): Int = {
    ((ts - startTimestamp) / batchTimeInterval).toInt
  }
  private def summarize(): Map[Int,Long] = {
    positions.map(_.min).zipWithIndex.map{ case (ts, phase) => (phase + 1, ts) }.toMap
  }
  def update(partitionId: Int, phaseId: Int, latestTimestamp: Long): Option[Map[Int,Long]] = {
    val prev = positions(phaseId)(partitionId)
    positions(phaseId)(partitionId) = latestTimestamp
    if (batchId(prev) != batchId(latestTimestamp)) {
      Some(summarize())
    } else {
      None
    }
  }
}
