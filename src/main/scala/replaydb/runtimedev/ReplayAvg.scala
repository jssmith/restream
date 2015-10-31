package replaydb.runtimedev

class ReplayAvg(counterSrc: => ReplayCounter) {
  val sum: ReplayCounter = counterSrc
  val ct: ReplayCounter = counterSrc
  def add(value: Long, ts: Long) = {
    sum.add(value, ts)
    ct.add(1, ts)
  }
  def get(ts: Long): Option[Double] = {
    val n = ct.get(ts)
    if (n > 0) {
      Some(sum.get(ts).toDouble / n.toDouble)
    } else {
      None
    }
  }
}
