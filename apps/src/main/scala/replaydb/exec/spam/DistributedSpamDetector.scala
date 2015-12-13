package replaydb.exec.spam

import replaydb.runtimedev._
import replaydb.service.ClientGroup
import replaydb.service.driver.{Hosts, InitReplayCommand, RunConfiguration}
import replaydb.util.{EventRateEstimator, LoggerConfiguration}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// TODO this needs a way to time how long things take to do performance checks, like the serial/parallel ones

object DistributedSpamDetector extends App {
  if (args.length != 5) {
    println(
      """Usage: DistributedSpamDetector spamDetector baseFilename numPartitions batchSize hosts
        |  Example values:
        |    spamDetector   = replaydb.exec.spam.SpamDetectorStats
        |    baseFilename   = ~/data/events.out
        |    numPartitions  = 4
        |    batchSize      = 50000
        |    hosts          = hosts.txt
      """.stripMargin)
    System.exit(1)
  }

  LoggerConfiguration.configureDriver()

  val spamDetector = Class.forName(args(0)).asInstanceOf[Class[HasRuntimeInterface]]
  val partitionFnBase = args(1)
  val numPartitions = args(2).toInt
  val batchSize = args(3).toInt
  val hostsFile = args(4)

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

  val numPhases = spamDetector.getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
    .newInstance(new ReplayStateFactory {
    override def getReplayMap[K, V: ClassTag](default: => V): ReplayMap[K, V] = null
    override def getReplayCounter: ReplayCounter = null
    override def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = null
  }).getRuntimeInterface.numPhases

  val runConfiguration = new RunConfiguration(numPartitions = numPartitions, numPhases = numPhases, hosts,
    startTimestamp = startTime, batchTimeInterval = batchTimeInterval, approxBatchSize = batchSize)

  println("connecting...")
  val clients = new ClientGroup(runConfiguration)
  clients.connect(hosts)

  println("starting replay...")
  for (i <- hostFiles.indices) {
    clients.issueCommand(i, new InitReplayCommand(i, hostFiles(i), spamDetector, runConfiguration))
  }
  clients.closeWhenDone()
}
