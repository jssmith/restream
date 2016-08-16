package replaydb.exec.spam.runAnalysis

case class LoggedRunConfiguration(name: String, numHosts: Int, numPartitions: Int,
                                  iteration: Int, detector: String, alpha: Float, uuid: String) {
  def getCSV: String = {
    s"$uuid,$detector,$numHosts,$numPartitions,$alpha"
  }

  def getLogName: String = {
    uuid
  }
}
