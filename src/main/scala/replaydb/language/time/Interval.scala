package replaydb.language.time

object Interval {
  def apply(minTime: TimeOffset, maxTime: TimeOffset): Interval = {
    new Interval(minTime, maxTime)
  }
}

class Interval(minTime: TimeOffset, maxTime: TimeOffset) {
  override def toString: String = {
    s"$minTime to $maxTime"
  }
}
