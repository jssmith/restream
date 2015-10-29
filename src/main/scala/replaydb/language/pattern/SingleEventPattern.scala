package replaydb.language.pattern

import replaydb.language.EventStore
import replaydb.language.event.Event

// Assuming only a single event type allowed for now...
class SingleEventPattern(event: Event) extends Pattern {
  type T = Event
  // TODO how to represent the constraints on the values of fields if there are variables?

  override def get_matches: Set[Event] = {
    EventStore.get.filter(_.equals(event))
  }

  override def toString: String = {
    event.toString
  }
}
