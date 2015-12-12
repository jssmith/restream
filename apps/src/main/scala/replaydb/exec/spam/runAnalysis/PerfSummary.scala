package replaydb.exec.spam.runAnalysis

case class PerfSummary(
  readerThreadMs: Long,
  bossThreadMs: Long,
  workerThreadMs: Long,
  phaseThreadMs: Long,
  kryoSendMs: Long,
  kryoRecvMs: Long,
  kryoSendBytes: Long,
  kryoRecvBytes: Long
) {
  def getCsv: String = {
    s"$readerThreadMs,$phaseThreadMs,$bossThreadMs,$workerThreadMs,$kryoSendMs,$kryoRecvMs,$kryoSendBytes,$kryoRecvBytes"
  }
}
