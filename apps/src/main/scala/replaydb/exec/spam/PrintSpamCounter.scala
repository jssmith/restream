package replaydb.exec.spam

import replaydb.event.Event

case class PrintSpamCounter(ts: Long) extends Event
