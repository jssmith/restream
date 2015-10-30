package replaydb.language.bindings

import replaydb.language.time.{Timestamp, TimeOffset}

class TimeIntervalBinding(val relativeTo: NamedBinding[Timestamp],
                          _min: TimeOffset,
                          _max: TimeOffset)
  extends Binding[Timestamp] {

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
