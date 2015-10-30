package replaydb.language.pattern

import replaydb.language.time.{TimeOffset, Interval}

class SequencePatternWithoutInterval(parent: SequencePattern) {

  def after(minTime: TimeOffset): SequencePattern = {
    // TODO possibly not the best way to represent an unbounded interval...
    // for both of these should probably have some special TimePeriod value
    within(Interval(minTime, new TimeOffset(Long.MaxValue)))
  }

  def within(maxTime: TimeOffset): SequencePattern = {
    within(Interval(new TimeOffset(0L), maxTime))
  }

  def within(interval: Interval): SequencePattern = {
    parent.setNextInterval(interval)
    parent
  }

}
