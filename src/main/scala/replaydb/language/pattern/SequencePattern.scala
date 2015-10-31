package replaydb.language.pattern

import replaydb.language.time._
import replaydb.language.Match
import replaydb.language.bindings.{Binding, TimeNowBinding, TimeIntervalBinding, NamedTimeIntervalBinding}
import replaydb.language.event.Event
import replaydb.language.time.{TimeOffset, Interval}

class SequencePattern[T <: Event](last: SingleEventPattern[T], others: SingleEventPattern[T]*) extends Pattern[T] {

  var patterns = Seq(others:_*) :+ last

  // TODO this is kind of weird right now... trying to set a binding relative to now, until the beginning of time
  if (last.interval == null && others.isEmpty) {
    last.interval = new TimeIntervalBinding(Binding.now, TimeOffset.min, 0)
  }

  def followedBy[S >: T <: Event](p: SingleEventPattern[S]): SequencePattern[S] = {
//    patterns :+= p
    new SequencePattern[S](p, patterns:_*)
//    new SequencePatternWithoutInterval(this)
//    this
  }

//  def withinLast(maxTimeAgo: TimeOffset): SequencePattern = {
//    parent.interval.min = -maxTimeAgo
//    this
//  }
//
  def after(minTime: TimeOffset): SequencePattern[T] = {
    // TODO possibly not the best way to represent an unbounded interval...
    // for both of these should probably have some special TimePeriod value
    within(minTime, TimeOffset.max)
  }

  def within(maxTime: TimeOffset): SequencePattern[T] = {
    within(new TimeOffset(0L), maxTime)
  }

  def within(interval: Interval): SequencePattern[T] = {
    within(interval.minTime, interval.maxTime)
  }

  def within(min: TimeOffset, max: TimeOffset): SequencePattern[T] = {
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

  override def getMatches: Seq[Match[T]] = {
    // TODO this is clearly not correct
//    var ret = Seq[Match[Event]]()
//    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
//      if (mtch1.ts < mtch2.ts)
//        ret += mtch1
//    }
//    ret
    Seq()
  }

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

