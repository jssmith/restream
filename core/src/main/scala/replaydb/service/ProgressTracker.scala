package replaydb.service

import replaydb.service.driver.RunConfiguration

class ProgressTracker(runConfiguration: RunConfiguration) {
  import runConfiguration._

  // Number of batches the first phase is allowed to run ahead as compared
  // to the last phase
  val FirstPhaseBatchAllowance = 3 + numPhases

  private val allowedPositions = Array.ofDim[Long](numPhases,numPartitions)
  for (i <- 0 until numPartitions) {
    allowedPositions(0)(i) = startTimestamp + FirstPhaseBatchAllowance * batchTimeInterval
  }
  private def batchId(ts: Long): Int = {
    ((ts - startTimestamp) / batchTimeInterval).toInt
  }
  private def summarize(): Map[Int,Long] = {
    //allowedPositions.map(_.min).zipWithIndex.map{ case (ts, phase) => (phase + 1, ts) }.toMap
    allowedPositions.map(_.min).zipWithIndex.map(_.swap).toMap
  }
  var lastSummary = summarize()
  def update(partitionId: Int, phaseId: Int, latestTimestamp: Long): Option[Map[Int,Long]] = {
    if (phaseId == numPhases-1) {
      allowedPositions(0)(partitionId) = latestTimestamp + FirstPhaseBatchAllowance * batchTimeInterval
    } else {
      allowedPositions(phaseId + 1)(partitionId) = latestTimestamp
    }
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
