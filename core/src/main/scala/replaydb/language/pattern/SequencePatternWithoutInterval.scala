package replaydb.language.pattern

import replaydb.language.time.{TimeOffset, Interval}

//class SequencePatternWithoutInterval(parent: SequencePattern) {

//  // TODO these should all be restricted to positive (forward in time) only?
//
//  def after(minTime: TimeOffset): SequencePattern = {
//    // TODO possibly not the best way to represent an unbounded interval...
//    // for both of these should probably have some special TimePeriod value
//    within(minTime, new TimeOffset(Long.MaxValue))
//  }
//
//  def within(maxTime: TimeOffset): SequencePattern = {
//    within(new TimeOffset(0L), maxTime)
//  }
//
//  def within(interval: Interval): SequencePattern = {
//    within(interval.minTime, interval.maxTime)
//  }
//
//  def within(minTime: TimeOffset, maxTime: TimeOffset): SequencePattern = {
//    parent.setNextInterval(minTime, maxTime)
//    parent
//  }

//}
