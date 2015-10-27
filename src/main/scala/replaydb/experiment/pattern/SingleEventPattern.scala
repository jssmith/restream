package replaydb.experiment.pattern

import replaydb.experiment.EventStore
import replaydb.experiment.event.Event

/**
 * Created by erik on 10/27/15.
 */
// Assuming only a single event type allowed for now...
class SingleEventPattern(event: Event) extends Pattern {
  // TODO how to represent the constraints on the values of fields if there are variables?

  override def get_matches: Set[Event] = {
    EventStore.get.filter(_.equals(event))
  }

  override def toString: String = {
    event.toString
  }
}
