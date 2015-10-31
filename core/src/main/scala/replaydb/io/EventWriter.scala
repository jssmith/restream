package replaydb.io

import replaydb.event.Event

trait EventWriter {
  def write(e: Event)
  def close(): Unit
}
