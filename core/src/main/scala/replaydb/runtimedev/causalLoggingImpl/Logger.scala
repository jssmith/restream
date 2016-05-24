package replaydb.runtimedev.causalLoggingImpl


import org.slf4j.LoggerFactory

trait Logger {

  val logger = com.typesafe.scalalogging.Logger(LoggerFactory.getLogger(classOf[Logger]))

  var lastModified = -1L

  def logWrite(ts: Long): Unit = {
    if (lastModified != -1L && lastModified != ts) {
      logger.info(s"$lastModified,$ts")
    }
    if (ts < lastModified) {
      throw new RuntimeException("out of order")
    }
    lastModified = ts
  }

  def logRead(ts: Long): Unit = {
    if (lastModified != -1L && lastModified != ts) {
      if (ts < lastModified) {
        throw new RuntimeException("out of order")
      }
      logger.info(s"$lastModified,$ts")
    }
  }

}
