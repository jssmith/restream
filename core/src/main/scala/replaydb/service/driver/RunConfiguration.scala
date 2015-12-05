package replaydb.service.driver

import replaydb.service.driver.Hosts.HostConfiguration

case class RunConfiguration(numPartitions: Int,
                            numPhases: Int,
                            hosts: Array[HostConfiguration],
                            startTimestamp: Long,
                            batchTimeInterval: Long)
