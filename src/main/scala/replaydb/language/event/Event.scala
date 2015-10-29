package replaydb.language.event

import replaydb.language.Timestamp
import replaydb.language.pattern.SingleEventPattern

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