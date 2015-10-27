package replaydb.experiment.event

import replaydb.experiment.Timestamp
import replaydb.experiment.pattern.SingleEventPattern

/**
 * Created by erik on 10/27/15.
 */
object Event {
  implicit def patternFromEvent(e: Event): SingleEventPattern = {
    new SingleEventPattern(e)
  }
}

class Event(val ts: Timestamp) {

}