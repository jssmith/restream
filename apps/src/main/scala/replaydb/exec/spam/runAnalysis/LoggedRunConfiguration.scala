package replaydb.exec.spam.runAnalysis

case class LoggedRunConfiguration(numHosts: Int, numPartitions: Int, iteration: Int) {
  def getCSV: String = {
    s"$numHosts,$numPartitions"
  }
}
