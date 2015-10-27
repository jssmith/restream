package replaydb.experiment.pattern

import replaydb.experiment.Interval
import replaydb.experiment.event.Event
import replaydb.experiment.rcollection.PatternRCollection

/**
 * Created by erik on 10/27/15.
 */
object Pattern {
  implicit def RCollectionFromPattern(p: Pattern): PatternRCollection = {
    new PatternRCollection(p)
  }
}

abstract class Pattern {

  def followed_by(p: Pattern): Pattern = {
    new SequencePattern(this, p)
  }

  def with_interval(interval: Interval): IntervalPattern = {
    new IntervalPattern(this, interval)
  }

  def get_matches: Set[Event]
}