package replaydb.language.pattern

import replaydb.language.Interval
import replaydb.language.event.Event

class IntervalPattern(basePattern: Pattern, interval: Interval) extends Pattern {

  override def toString: String = {
    basePattern.toString + s", from $interval"
  }

  override def get_matches: Set[Event] = {
    Set() // not yet implemented...
  }
}
