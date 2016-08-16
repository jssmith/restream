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
    if (x.length > 0) {
      x.max.toDouble * x.length / x.sum
    } else {
      0D
    }
  }
  def getCsv: String = {
    s"$readerThreadMs,$phaseThreadMs,$bossThreadMs,$workerThreadMs,$kryoSendMs,$kryoRecvMs,${sum(kryoSendBytes)},${sum(kryoRecvBytes)},${skew(kryoSendBytes)},${skew(kryoRecvBytes)}"
  }
}

object PerfSummary {
  val EMPTY = PerfSummary(
    readerThreadMs = 0L,
    bossThreadMs = 0L,
    workerThreadMs = 0L,
    phaseThreadMs = 0L,
    kryoSendMs = 0L,
    kryoRecvMs = 0L,
    kryoSendBytes = Array[Long](),
    kryoRecvBytes = Array[Long]()
  )
}