package replaydb.experiment

/**
 * Created by erik on 10/27/15.
 */
class Interval(t1: Timestamp, t2: Timestamp) {
  override def toString: String = {
    s"$t1 to $t2"
  }
}
