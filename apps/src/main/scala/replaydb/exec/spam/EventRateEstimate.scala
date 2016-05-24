package replaydb.exec.spam

import replaydb.util.EventRateEstimator

object EventRateEstimate extends App {

  val partitionFnBase = args(0)
  val numPartitions = args(1).toInt
  val r = EventRateEstimator.estimateRate(partitionFnBase, numPartitions)
  println(s"estimating rate for $partitionFnBase and $numPartitions partitions as ${r.startTime}, ${r.eventIntervalMs}")
}
