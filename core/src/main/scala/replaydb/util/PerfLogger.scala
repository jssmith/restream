package replaydb.util

import org.slf4j.LoggerFactory

object PerfLogger {
  protected val cpuLogger = LoggerFactory.getLogger("replaydb.perf.cpu")
  protected val gcLogger = LoggerFactory.getLogger("replaydb.perf.gc")
  protected val networkLogger = LoggerFactory.getLogger("replaydb.perf.net")

  def logCPU(msg: String): Unit = {
    cpuLogger.info(msg)
  }

  def logGc(msg: String): Unit = {
    gcLogger.info(msg)
  }

  def logNetwork(msg: String): Unit = {
    networkLogger.info(msg)
  }
}
