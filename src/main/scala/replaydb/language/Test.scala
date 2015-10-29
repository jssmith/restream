package replaydb.language

import replaydb.language.event.{Event, MessageEvent}
import replaydb.language.pattern.Pattern

class SpecificMessageEvent(sendID: Long, recvID: Long, ts: Timestamp)
  extends MessageEvent(sendID, recvID, ts) {

  override def toString: String = {
    s"SpecificMessageEvent(from $sendID to $recvID at $ts)"
  }
}

class OtherEvent(id: Long, ts: Timestamp) extends Event(ts) {
  override def toString: String = {
    s"OtherEvent(from $id at $ts"
  }
}

object Test extends App {

  val myEvent = new MessageEvent(0, 1, Timestamp.any)
  val mySecondEvent = new MessageEvent(1, 0, Timestamp.any)
  val myThirdEvent = new MessageEvent(2, 3, Timestamp.any)
  val mySpecificEvent = new SpecificMessageEvent(4, 5, Timestamp.any)
  val myOtherEvent = new OtherEvent(5, Timestamp.any)
  val p = (myEvent or myOtherEvent) followed_by (mySecondEvent or mySpecificEvent) followed_by myThirdEvent
  println("Pattern is: " + p)
//  println("Found " + p.count + " matches")
//  for (m <- p) { println(m) }
//  val mapped = p.map((e: Event) => e.toString + " was matched!")
//  for (m <- mapped) { println(m) }

}

object EventStore {
  implicit def longToTimestamp(l: Long): Timestamp = { new Timestamp(l) }

  val EventStore = Set[Event](MessageEvent(0, 1, 0L), MessageEvent(2, 3, 1L), MessageEvent(1, 0, 2L))

  def get: Set[Event] = { EventStore }
}















