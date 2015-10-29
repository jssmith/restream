package replaydb.language.pattern

import replaydb.language.Interval
import replaydb.language.event.Event
import replaydb.language.rcollection.PatternRCollection

object Pattern {
  implicit def RCollectionFromPattern(p: Pattern): PatternRCollection = {
    new PatternRCollection(p)
  }

  implicit def SequencePatternFromPattern(p: Pattern): SequencePattern = {
    new SequencePattern(p)
  }
}

abstract class Pattern {
  type T

  def with_interval(interval: Interval): IntervalPattern = {
    new IntervalPattern(this, interval)
  }

  def get_matches: Set[T]
}