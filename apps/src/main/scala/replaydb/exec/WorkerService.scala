package replaydb.exec

import org.slf4j.LoggerFactory
import replaydb.exec.spam.SpamDetectorStats
import replaydb.runtimedev.{ReplayTimestampLocalMap, ReplayCounter, ReplayMap, ReplayStateFactory}
import replaydb.service.Server
import replaydb.util.LoggerConfiguration

import scala.reflect.ClassTag

object WorkerService extends App {
  val logger = LoggerFactory.getLogger(WorkerService.getClass)
  if (args.length != 1) {
    println(
      """Usage: WorkerService port
        |  Suggested values: port = 5567
      """.stripMargin)
    System.exit(1)
  }
  LoggerConfiguration.configureWorker(args(0))
  val port = args(0).toInt

  val server = new Server(port, new SpamDetectorStats(new ReplayStateFactory {
    override def getReplayMap[K, V: ClassTag](default: => V): ReplayMap[K, V] = null
    override def getReplayCounter: ReplayCounter = null
    override def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = null
  }))
  server.run()
}
