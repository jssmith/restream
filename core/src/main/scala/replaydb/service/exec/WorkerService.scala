package replaydb.service.exec

import org.slf4j.LoggerFactory
import replaydb.service.Server

object WorkerService extends App {
  val logger = LoggerFactory.getLogger(WorkerService.getClass)
  if (args.length != 1) {
    println(
      """Usage: WorkerService port
        |  Suggested values: port = 5567
      """.stripMargin)
    System.exit(1)
  }

  val port = args(0).toInt
  val server = new Server(port)
  server.run()
}
