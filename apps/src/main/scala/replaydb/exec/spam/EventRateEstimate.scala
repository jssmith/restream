package replaydb.exec.spam

import replaydb.util.EventRateEstimator

object EventRateEstimate extends App {

  val partitionFnBase = args(0)
  val numPartitions = args(1).toInt
  val eventsPerPartition = args(2).toInt
  val r = EventRateEstimator.estimateRate(partitionFnBase, numPartitions, eventsPerPartition)
  println(r.eventIntervalMs.toLong)
}
