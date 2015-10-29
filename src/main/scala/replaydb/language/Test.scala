package replaydb.language

import replaydb.language.event.{Event, MessageEvent}

object Test extends App {

  val myEvent = new MessageEvent(0, 1, Timestamp.any)
  val mySecondEvent = new MessageEvent(1, 0, Timestamp.any)
  val p = myEvent followed_by mySecondEvent
  println("Pattern is: " + p)
  println("Found " + p.count + " matches")
  for (m <- p) { println(m) }
  val mapped = p.map((e: Event) => e.toString + " was matched!")
  for (m <- mapped) { println(m) }

}

object EventStore {
  implicit def longToTimestamp(l: Long): Timestamp = { new Timestamp(l) }

  val EventStore = Set[Event](MessageEvent(0, 1, 0L), MessageEvent(2, 3, 1L), MessageEvent(1, 0, 2L))

  def get: Set[Event] = { EventStore }
}















