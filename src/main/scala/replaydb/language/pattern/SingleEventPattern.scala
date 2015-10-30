package replaydb.language.pattern

import replaydb.language.bindings.TimeIntervalBinding
import replaydb.language.Match
import replaydb.language.event.Event

// Matches a single event (not a sequence) though you may specify multiple
// match possibilities
class SingleEventPattern[T <: Event](val event: T) extends Pattern {

//  var _interval: TimeIntervalBinding = _
  def interval = event.ts
  def interval_=(binding: TimeIntervalBinding) = event.ts = binding

  def followedBy(p: SingleEventPattern[_ <: Event]): SequencePattern = {
    val sp = new SequencePattern(this)
    sp followedBy p
  }

//  def or[S >: T <: Event](p: S): SingleEventPattern[S] = {
//    new SingleEventPattern(p, events.toSeq:_*)
//  }

  override def getMatches: Seq[Match[T]] = {
//    EventStore.get.filter(_.equals(event))
    Seq()
  }

  override def toString: String = {
//    if (events.size == 1)
    event.toString // + " " + (if (_interval == null) "" else _interval)
//    else
//      events.mkString("(", " OR ", ")") + " " + (if (_interval == null) "" else _interval)
  }
}
