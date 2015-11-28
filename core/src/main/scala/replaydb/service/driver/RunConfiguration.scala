package replaydb.service.driver

case class RunConfiguration(numPartitions: Int, numPhases: Int, startTimestamp: Long, batchTimeInterval: Long)
