package replaydb

import replaydb.event.Event

trait EventSource {
  def genEvents(n: Int, f: Event=>Unit)
}
