package replaydb.exec.spam

import replaydb.runtimedev.HasRuntimeInterface
import replaydb.service.Server

trait TestRunner {
  object DataDesc {
    val SHORT = "500k"
    val MEDIUM = "5m"
    val LONG = "50m"
  }
  protected def runSpamDetector[T<:HasRuntimeInterface](numServers: Int, spamDetectorClass: Class[T], dataDesc: String) = {
    val startPort = 5567
    val ports = startPort until (startPort + numServers)
    val servers = ports.map(new Server(_))
    servers.foreach(_.run())
    try {
      DistributedSpamDetector.main(Array[String](spamDetectorClass.getName,
        s"${System.getProperty("user.home")}/tmp/events-$dataDesc-split-$numServers/events.out",
        s"$numServers", "50000", s"src/test/resources/hosts-$numServers.txt")
      )
    } finally {
      servers.foreach(_.close())
    }
  }

}
