package replaydb.exec.spam.runAnalysis

case class PerfSummary(
  readerThreadMs: Long,
  bossThreadMs: Long,
  workerThreadMs: Long,
  phaseThreadMs: Long,
  kryoSendMs: Long,
  kryoRecvMs: Long,
  kryoSendBytes: Array[Long],
  kryoRecvBytes: Array[Long]
) {
  private def sum(x: Array[Long]): Double = {
    x.sum
  }
  private def skew(x: Array[Long]): Double = {
    x.max.toDouble * x.length / x.sum
  }
  def getCsv: String = {
    s"$readerThreadMs,$phaseThreadMs,$bossThreadMs,$workerThreadMs,$kryoSendMs,$kryoRecvMs,${sum(kryoSendBytes)},${sum(kryoRecvBytes)},${skew(kryoSendBytes)},${skew(kryoRecvBytes)}"
  }
}
