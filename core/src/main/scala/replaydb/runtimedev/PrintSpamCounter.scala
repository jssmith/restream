package replaydb.runtimedev

import replaydb.event.Event

// TODO this should get moved back to apps but I need to access it from core right
// now to be able to inject it from within WorkerServiceHandler

case class PrintSpamCounter(ts: Long) extends Event
