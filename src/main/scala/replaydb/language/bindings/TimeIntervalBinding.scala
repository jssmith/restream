package replaydb.language.bindings

import replaydb.language.time.{Timestamp, TimeOffset}
import replaydb.language.time._

class TimeIntervalBinding(val relativeTo: NamedBinding[Timestamp],
                          _min: TimeOffset,
                          _max: TimeOffset)
  extends Binding[Timestamp] {

  def this(_min: TimeOffset, _max: TimeOffset) = this(Binding.now, _min, _max)

  var min = _min
  var max = _max

  def maxTime_=(offset: TimeOffset) = offset
  def maxTime = _max

  def minTime_=(offset: TimeOffset) = offset
  def minTime = _min

  override def toString: String = {
    s"between $min and $max from ${relativeTo.name}"
  }

}

object TimeIntervalBinding {

  // Returns a TimeIntervalBinding matching against NOW which matches everything
  // back to the beginning of time
  def global: TimeIntervalBinding = {
    new TimeIntervalBinding(TimeOffset.min, 0)
  }

  def global(name: String): NamedTimeIntervalBinding = {
    new NamedTimeIntervalBinding(name, global)
  }

  // named interval relative to NOW
  def apply(name: String, min: TimeOffset, max: TimeOffset): NamedTimeIntervalBinding = {
    new NamedTimeIntervalBinding(name, apply(min, max))
  }

  def apply(min: TimeOffset, max: TimeOffset): TimeIntervalBinding = {
    new TimeIntervalBinding(min, max)
  }

  def apply(name: String, relativeTo: NamedBinding[Timestamp],
            min: TimeOffset, max: TimeOffset): NamedTimeIntervalBinding = {
    new NamedTimeIntervalBinding(name, relativeTo, min, max)
  }

  def apply(relativeTo: NamedBinding[Timestamp], min: TimeOffset,
            max: TimeOffset): TimeIntervalBinding = {
    new TimeIntervalBinding(relativeTo, min, max)
  }

}
