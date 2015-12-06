package replaydb.util

import org.slf4j.LoggerFactory

object PerfLogger {
  protected val logger = LoggerFactory.getLogger("replaydb.perf")
  def log(msg: String): Unit = {
    logger.info(msg)
  }
}
