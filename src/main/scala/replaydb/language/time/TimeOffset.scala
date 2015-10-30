package replaydb.language.time

class TimeOffset(lengthMillis: Long) {

  override def toString: String = {
    // TODO probably not what we want
    if (lengthMillis == 0) "0s" else nicePrint(lengthMillis)
  }

  def nicePrint(millis: Long): String = {
    if (millis / MillisPerWeek >= 1) {
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
    }
  }

}
