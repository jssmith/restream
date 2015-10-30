package replaydb.language.time

class TimeOffset(val lengthMillis: Long) {

  override def toString: String = {
    if (lengthMillis == 0) "0s" else nicePrint(lengthMillis)
  }

  def nicePrint(_millis: Long): String = {
    var prefix = ""
    var millis = _millis
    if (millis < 0) {
      prefix = "-"
      millis = if (millis == Long.MinValue) Long.MaxValue else -millis
    }
    prefix + (if (millis / MillisPerWeek >= 1) {
      s"${millis / MillisPerWeek}w${nicePrint(millis % MillisPerWeek)}"
    } else if (millis / MillisPerDay >= 1) {
      s"${millis / MillisPerDay}d${nicePrint(millis % MillisPerDay)}"
    } else if (millis / MillisPerHour >= 1) {
      s"${millis / MillisPerHour}h${nicePrint(millis % MillisPerHour)}"
    } else if (millis / MillisPerMinute >= 1) {
      s"${millis / MillisPerMinute}m${nicePrint(millis % MillisPerMinute)}"
    } else if (millis / MillisPerSecond >= 1) {
      s"${millis / MillisPerSecond}s${nicePrint(millis % MillisPerSecond)}"
    } else if (millis > 0) {
      s"${millis}ms"
    } else {
      ""
    })
  }

  def unary_+ = this
  def unary_- = new TimeOffset(-lengthMillis)

  def +(x: TimeOffset): TimeOffset = {
    new TimeOffset(lengthMillis + x.lengthMillis)
  }

  def -(x: TimeOffset): TimeOffset = {
    new TimeOffset(lengthMillis - x.lengthMillis)
  }

}

object TimeOffset {

  val MinTime: TimeOffset = new TimeOffset(Long.MinValue) {
    override def toString: String = {
      "min"
    }

    override def unary_- = MaxTime
  }

  val MaxTime: TimeOffset = new TimeOffset(Long.MaxValue) {
    override def toString: String = {
      "max"
    }

    override def unary_- = MinTime
  }

  def max = MaxTime
  def min = MinTime
}
