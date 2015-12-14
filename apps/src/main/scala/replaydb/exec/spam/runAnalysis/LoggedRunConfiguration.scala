package replaydb.exec.spam.runAnalysis

case class LoggedRunConfiguration(numHosts: Int, numPartitions: Int,
                                  iteration: Int, detector: String) {
  def getCSV: String = {
    s"$detector,$numHosts,$numPartitions"
  }
}
