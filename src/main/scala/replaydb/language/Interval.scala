package replaydb.language

class Interval(t1: Timestamp, t2: Timestamp) {
  override def toString: String = {
    s"$t1 to $t2"
  }
}
