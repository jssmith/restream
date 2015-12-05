package replaydb.util

package object time {

  implicit def intToTimeWrappedDouble(v: Int): TimeWrappedDouble = {
    new TimeWrappedDouble(v)
  }

  implicit def longToTimeWrappedDouble(v: Long): TimeWrappedDouble = {
    new TimeWrappedDouble(v)
  }

  implicit def floatToTimeWrappedDouble(v: Float): TimeWrappedDouble = {
    new TimeWrappedDouble(v)
  }

  implicit def doubleToTimeWrappedDouble(v: Double): TimeWrappedDouble = {
    new TimeWrappedDouble(v)
  }

  val MillisPerSecond = 1000
  val MillisPerMinute = 60 * MillisPerSecond
  val MillisPerHour = 60 * MillisPerMinute
  val MillisPerDay = 24 * MillisPerHour
  val MillisPerWeek = 7 * MillisPerDay
}

class TimeWrappedDouble(length: Double) {
  def weeks: Long = Math.round(length * time.MillisPerWeek)

  def days: Long = Math.round(length * time.MillisPerDay)

  def hours: Long = Math.round(length * time.MillisPerHour)

  def minutes: Long = Math.round(length * time.MillisPerMinute)

  def seconds: Long = Math.round(length * time.MillisPerSecond)

  def millis: Long = Math.round(length)
}