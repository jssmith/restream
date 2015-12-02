package replaydb.runtimedev

import replaydb.event.Event

// TODO this should get moved back to apps

case class PrintSpamCounter(ts: Long) extends Event
