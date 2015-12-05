package replaydb.exec.spam

import replaydb.runtimedev.{ReplayMap, ReplayCounter, ReplayTimestampLocalMap, ReplayStateFactory}
import replaydb.service.{KryoCommandEncoder, ClientGroup}
import replaydb.service.driver.{RunConfiguration, Hosts, InitReplayCommand}
import replaydb.util.EventRateEstimator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

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

  if (numHosts != numPartitions) {
    throw new RuntimeException("Must have same number of hosts and partitions")
  }

  val filenames = (0 until numPartitions).map(i => s"$partitionFnBase-$i").toArray

//  // round robin assignment of files to hosts
//  val hostFiles = new Array[ArrayBuffer[(Int,String)]](numHosts)
//  for (i <- hostFiles.indices) {
//    hostFiles(i) = ArrayBuffer[(Int,String)]()
//  }
//  for (i <- filenames.indices) {
//    hostFiles(i % numHosts) += i -> filenames(i)
//  }

  // estimate the event rate so that we can set a batch time range
  val r = EventRateEstimator.estimateRate(partitionFnBase, numPartitions)
  val startTime = r.startTime
  val batchTimeInterval = (batchSize * r.eventIntervalMs).toLong
  println(s"rate estimate $r")

  val numPhases = new SpamDetectorStats(new ReplayStateFactory {
    override def getReplayMap[K, V: ClassTag](default: => V): ReplayMap[K, V] = null
    override def getReplayCounter: ReplayCounter = null
    override def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = null
  }).getRuntimeInterface.numPhases

  val runConfiguration = new RunConfiguration(numPartitions = numPartitions, numPhases = numPhases, hosts,
    startTimestamp = startTime, batchTimeInterval = batchTimeInterval)

  println("connecting...")
  val clients = new ClientGroup(runConfiguration)
  clients.connect(hosts)

  println("starting replay...")
  for (i <- filenames.indices) {
    clients.issueCommand(i, new InitReplayCommand(i, filenames(i), classOf[SpamDetectorStats], i, runConfiguration))
  }
  clients.closeWhenDone()
}
