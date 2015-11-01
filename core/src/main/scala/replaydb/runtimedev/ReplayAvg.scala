package replaydb.runtimedev

case class ReplayAvg(sum: Long, ct: Long) {
  def getAvg(): Option[Double] = {
    if (ct > 0) {
      Some(sum.toDouble / ct.toDouble)
    } else {
      None
    }
  }
}

object ReplayAvg {
  def default = new ReplayAvg(0, 0)
  def add(value: Long) = (prev: ReplayAvg) => new ReplayAvg(prev.sum + value, prev.ct + 1)
}
