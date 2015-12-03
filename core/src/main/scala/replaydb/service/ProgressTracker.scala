package replaydb.service

import replaydb.service.driver.RunConfiguration

class ProgressTracker(runConfiguration: RunConfiguration) {
  import runConfiguration._

  private val allowedPositions = Array.ofDim[Long](numPhases,numPartitions)
  for (i <- 0 until numPartitions) {
    allowedPositions(0)(i) = Long.MaxValue // let the first phase do whatever it wants
  }
  private def batchId(ts: Long): Int = {
    ((ts - startTimestamp) / batchTimeInterval).toInt
  }
  private def summarize(): Map[Int,Long] = {
    //allowedPositions.map(_.min).zipWithIndex.map{ case (ts, phase) => (phase + 1, ts) }.toMap
    allowedPositions.map(_.min).zipWithIndex.map{ case (ts, phase) => (phase + 1, ts) }.toMap
  }
  var lastSummary = summarize()
  def update(partitionId: Int, phaseId: Int, latestTimestamp: Long): Option[Map[Int,Long]] = {
    if (phaseId == numPhases) {
      return None // last phase, nothing to do
    }
    val prev = allowedPositions(phaseId)(partitionId)
    allowedPositions(phaseId)(partitionId) = latestTimestamp
    val summary = summarize()
    for ((k, v) <- summary) {
      if (lastSummary(k) != v) {
        lastSummary = summary
        return Some(summary)
      }
    }
    None
  }
}
