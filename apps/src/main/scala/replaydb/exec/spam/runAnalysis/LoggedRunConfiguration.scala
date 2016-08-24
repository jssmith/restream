package replaydb.exec.spam.runAnalysis

case class LoggedRunConfiguration(name: String, numHosts: Int, numPartitions: Int,
                                  iteration: Int, detector: String, alpha: Float,
                                  uuid: String, batchSize: Int) {
  def getCSV: String = {
    s"$uuid,$detector,$numHosts,$numPartitions,$alpha,$batchSize"
  }

  def getLogName: String = {
    uuid
  }
}
