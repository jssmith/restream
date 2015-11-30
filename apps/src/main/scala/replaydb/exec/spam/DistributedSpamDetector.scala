package replaydb.exec.spam


import replaydb.service.ClientGroup
import replaydb.service.driver.{RunConfiguration, Hosts, InitReplayCommand}
import replaydb.util.EventRateEstimator

import scala.collection.mutable.ArrayBuffer

object DistributedSpamDetector extends App {
  if (args.length != 4) {
    println(
      """Usage: DistributedSpamDetector baseFilename numPartitions batchSize hosts
        |  Suggested values: numPartitions = 4, batchSize = 50000 hosts = hosts.txt
      """.stripMargin)
    System.exit(1)
  }

  val partitionFnBase = args(0)
  val numPartitions = args(1).toInt
  val batchSize = args(2).toInt
  val hostsFile = args(3)

  val hosts = Hosts.fromFile(hostsFile)
  val numHosts = hosts.length

  val filenames = (0 until numPartitions).map(i => s"$partitionFnBase-$i").toArray

  // round robin assignment of files to hosts
  val hostFiles = new Array[ArrayBuffer[(Int,String)]](numHosts)
  for (i <- hostFiles.indices) {
    hostFiles(i) = ArrayBuffer[(Int,String)]()
  }
  for (i <- filenames.indices) {
    hostFiles(i % numHosts) += i -> filenames(i)
  }

  // estimate the event rate so that we can set a batch time range
  val r = EventRateEstimator.estimateRate(partitionFnBase, numPartitions)
  val startTime = r.startTime
  val batchTimeInterval = (batchSize * r.eventIntervalMs).toLong
  println(s"rate estimate $r")
  val runConfiguration = new RunConfiguration(numPartitions = numPartitions, numPhases = 5,
    startTimestamp = startTime, batchTimeInterval = batchTimeInterval)

  println("connecting...")
  val clients = new ClientGroup(runConfiguration)
  clients.connect(hosts)

  println("starting replay...")
  for (i <- hostFiles.indices) {
    clients.issueCommand(i, new InitReplayCommand(hostFiles(i).toMap, classOf[SpamDetectorStatsParallel],r.startTime, batchTimeInterval, progressUpdateInterval = 100000))
  }
  clients.closeWhenDone()
}
