package replaydb.util

import java.io.File

import org.apache.log4j._

object LoggerConfiguration {

  def configureDriver(): Unit = {
    Logger.getRootLogger.setLevel(Level.DEBUG)
    Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)))
    Logger.getLogger("org.jboss.netty").setLevel(Level.INFO)
  }

  def configureWorker(processName: String): Unit = {
    val logsDir = System.getProperty("replaydb-logdir")
    if (logsDir == null) {
      throw new RuntimeException("must set system property replaydb-logdir")
    }
    val f = new File(logsDir)
    if (!f.exists() || !f.isDirectory()) {
      throw new RuntimeException(s"repladb-logdir must be a directory but is ${f.getPath}")
    }
    System.err.println(s"logs for $processName are going to directory $f")

    Logger.getRootLogger.setLevel(Level.DEBUG)
    Logger.getLogger("org.jboss.netty").setLevel(Level.INFO)

    def getAppender(suffix: String) = {
      new FileAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN),
        s"$logsDir/$processName.$suffix",
        false
      )
    }

    val mainAppender = getAppender("log")
    Logger.getRootLogger.addAppender(mainAppender)

    val errorsAppender = getAppender("err")
    errorsAppender.setThreshold(Level.WARN)
    Logger.getRootLogger.addAppender(errorsAppender)

    val perfAppender = getAppender("perf")
    Logger.getLogger("replaydb.perf").addAppender(perfAppender)
  }

}
