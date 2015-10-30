package replaydb.language.pattern

import replaydb.language.time._
import replaydb.language.Match
import replaydb.language.bindings.{Binding, TimeNowBinding, TimeIntervalBinding, NamedTimeIntervalBinding}
import replaydb.language.event.Event
import replaydb.language.time.{TimeOffset, Interval}

class SequencePattern(parent: SingleEventPattern[_ <: Event]) extends Pattern {

  var patterns: Seq[SingleEventPattern[_ <: Event]] = Seq(parent)
  // intervals[i] is the allowable time interval between patterns[i] and patterns[i-1] 
  var intervals: Seq[Interval] = Seq()

  // TODO this is kind of weird right now... trying to set a binding relative to now, until the beginning of time
  parent.interval = new TimeIntervalBinding(Binding.now, Long.MinValue.millis, 0.millis)

  def followedBy(p: SingleEventPattern[_ <: Event]): SequencePattern = {
    patterns :+= p
//    new SequencePatternWithoutInterval(this)
    this
  }

  var withinLastSet = false

  def withinLast(maxTimeAgo: TimeOffset): SequencePattern = {
    withinLastSet = true
    parent.interval.min = -maxTimeAgo
    this
  }

  // TODO should add checks to make sure intervals actually get set

  def after(minTime: TimeOffset): SequencePattern = {
    // TODO possibly not the best way to represent an unbounded interval...
    // for both of these should probably have some special TimePeriod value
    within(minTime, new TimeOffset(Long.MaxValue))
  }

  def within(maxTime: TimeOffset): SequencePattern = {
    within(new TimeOffset(0L), maxTime)
  }

  def within(interval: Interval): SequencePattern = {
    within(interval.minTime, interval.maxTime)
  }

  def within(min: TimeOffset, max: TimeOffset): SequencePattern = {
    val previousPattern = patterns.dropRight(1).last
    previousPattern.interval match {
      case b: NamedTimeIntervalBinding => patterns.last.interval =
        new TimeIntervalBinding(b, min, max)
      case b: TimeIntervalBinding => {
        val newBinding = new NamedTimeIntervalBinding(previousPattern.interval)
        previousPattern.interval = newBinding
        patterns.last.interval = new TimeIntervalBinding(newBinding, min, max)
      }
    }
    this
  }

  override def getMatches: Seq[Match[Event]] = {
    // TODO this is clearly not correct
//    var ret = Seq[Match[Event]]()
//    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
//      if (mtch1.ts < mtch2.ts)
//        ret += mtch1
//    }
//    ret
    Seq()
  }
  
//  def setNextInterval(interval: Interval): Unit = {
//    if (intervals.size == patterns.size) {
//      throw new RuntimeException("Can't have an interval without a corresponding pattern!")
//    }
//    intervals :+= interval
//  }

//  override def toString: String = {
//    patterns.head.toString +
//      (for ((p, i) <- patterns.tail zip intervals)
//      yield s" followed by $p within $i").mkString("")
//  }

//  def setNextInterval(min: TimeOffset, max: TimeOffset): Unit = {
//
//  }

  override def toString: String = {
    patterns.map(p => s"$p").mkString(" followed by ")
  }
}

