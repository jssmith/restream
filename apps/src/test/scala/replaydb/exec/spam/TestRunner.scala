package replaydb.exec.spam

import replaydb.runtimedev._
import replaydb.service.Server

import scala.reflect.ClassTag

trait TestRunner {
  object DataDesc {
    val SHORT = "500k"
    val MEDIUM = "5m"
    val LONG = "50m"
  }
  protected def runDistributedSpamDetector[T<:HasRuntimeInterface](numServers: Int, spamDetectorClass: Class[T], dataDesc: String) = {
    val startPort = 5567
    val ports = startPort until (startPort + numServers)

    val constructor = spamDetectorClass.getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
    val factory = new replaydb.runtimedev.ReplayStateFactory {
      override def getReplayMap[K, V: ClassTag](default: => V): ReplayMap[K, V] = null
      override def getReplayCounter: ReplayCounter = null
      override def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = null
    }
    val program = constructor.newInstance(factory)

    val servers = ports.map(new Server(_, program.asInstanceOf[RuntimeStats]))
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

  protected def runSerialSpamDetector[T<:HasRuntimeInterface](spamDetectorClass: Class[T], dataDesc: String): Unit = {
    SerialSpamDetector2.main(Array[String](spamDetectorClass.getName,
      s"${System.getProperty("user.home")}/tmp/events-$dataDesc-split-1/events.out-0"
    ))
  }

}
