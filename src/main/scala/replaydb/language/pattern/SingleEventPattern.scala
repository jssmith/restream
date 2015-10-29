package replaydb.language.pattern

import replaydb.language.{Match, EventStore}
import replaydb.language.event.Event

// Matches a single event (not a sequence) though you may specify multiple
// match possibilities
class SingleEventPattern[EventType <: Event](event: EventType, otherEvents: EventType*) extends Pattern {
  // TODO how to represent the constraints on the values of fields if there are variables?

  var events = Set(otherEvents:_*) + event

  def or[ET >: EventType <: Event](p: ET): SingleEventPattern[ET] = {
    new SingleEventPattern(p, events.toSeq:_*)
  }

  override def get_matches: Seq[Match[EventType]] = {
//    EventStore.get.filter(_.equals(event))
    Seq()
  }

  override def toString: String = {
    if (events.size == 1) event.toString else events.mkString("(", " OR ", ")")
  }
}
