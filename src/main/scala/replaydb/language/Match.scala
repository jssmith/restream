package replaydb.language

import replaydb.language.event.Event

class Match[+EventType <: Event](primaryEvent: EventType, eventSequence: Seq[EventType] = null) {

  val events = if (eventSequence == null) Seq(primaryEvent) else eventSequence

  def getPrimary: EventType = {
    primaryEvent
  }

  def getEvents: Seq[EventType] = {
    events
  }
}

object Match {
  implicit def matchToEvent[T <: Event](m: Match[T]): T = {
    m.getPrimary
  }
}

