package replaydb.experiment.pattern

import replaydb.experiment.Interval
import replaydb.experiment.event.Event

/**
 * Created by erik on 10/27/15.
 */
class IntervalPattern(basePattern: Pattern, interval: Interval) extends Pattern {

  override def toString: String = {
    basePattern.toString + s", from $interval"
  }

  override def get_matches: Set[Event] = {
    Set() // not yet implemented...
  }
}
