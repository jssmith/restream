package replaydb.exec.spam.runAnalysis

case class LoggedRunConfiguration(name: String, numHosts: Int, numPartitions: Int,
                                  iteration: Int, detector: String, alpha: Float) {
  def getCSV: String = {
    s"$detector,$numHosts,$numPartitions,$alpha"
  }

  def getLogName: String = {
    name.substring(0, name.length - ".timing".length) + "-log"
  }
}
