package replaydb.language.event

import replaydb.language.bindings.{TimeIntervalBinding, Binding}
import replaydb.language.pattern.SingleEventPattern
import replaydb.language.time.Timestamp

object Event {
  implicit def patternFromEvent[T <: Event](e: T): SingleEventPattern[T] = {
    new SingleEventPattern[T](e)
  }
}

abstract class Event(timestamp: TimeIntervalBinding) {

  var _ts = timestamp
  def ts = _ts
  def ts_=(binding: TimeIntervalBinding) = _ts = binding

}