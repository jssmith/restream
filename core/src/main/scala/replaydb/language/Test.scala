package replaydb.language

import replaydb.language.bindings._
import replaydb.language.time._
import replaydb.language.event.{VoteEvent, Event, MessageEvent}
import replaydb.language.pattern.{SequencePattern, Pattern}
import replaydb.language.time.Timestamp

//class SpecificMessageEvent(sendID: Long, recvID: Long, ts: Timestamp)
//  extends MessageEvent(sendID, recvID, ts) {
//
//  override def toString: String = {
//    s"SpecificMessageEvent(from $sendID to $recvID at $ts)"
//  }
//}
//
//class OtherEvent(id: Long, ts: Timestamp) extends Event(ts) {
//  override def toString: String = {
//    s"OtherEvent(from $id at $ts)"
//  }
//}

object Test extends App {

//  val bindA = Binding[Long]("A")
//  val bindB = Binding[Long]("B")
//  val bindT = Binding[Timestamp]("T")
//  val bindT2 = new NamedTimeIntervalBinding("T2", bindT, 5 minutes, 10 minutes)
//  val myEvent = new MessageEvent(bindA, bindB, bindT)
//  val mySecondEvent = new MessageEvent(bindB, bindA, bindT2)
//  val myThirdEvent = new MessageEvent(2L, 3L, new TimeIntervalBinding(bindT2, 5 minutes, 10 minutes))
////  val mySpecificEvent = new SpecificMessageEvent(4, 5, Timestamp.any)
////  val myOtherEvent = new OtherEvent(5, Timestamp.any)
//  val p = (myEvent followedBy mySecondEvent within 7.hours
//    followedBy myThirdEvent after 3.minutes)
//  println("Pattern is: " + p + "\n")

//  val bindA = Binding[Long]("A")
//  val bindB = Binding[Long]("B")
//  val myEvent = MessageEvent("A".bind, "B".bind)
//  val mySecondEvent = MessageEvent("B".bind, "A".bind)
//  val myThirdEvent = MessageEvent(2L, 3L)
//  val p = (myEvent followedBy mySecondEvent within (7.hours + 2.minutes)
//    followedBy myThirdEvent after 3.minutes) withinLast 2.days
//  println("Pattern is: " + p)

  val id = 314159L
  val p = VoteEvent("A".bind, id, TimeIntervalBinding.global("TA")) followedBy
    VoteEvent(id, "A".bind, TimeIntervalBinding("TB", "TA".bind, 0, TimeOffset.max))
  println("Pattern is: " + p)

  val p2 = VoteEvent("A".bind, id) followedBy VoteEvent(id, "A".bind) after 0.seconds
  println("Pattern2 is: " + p2)

  val p3 = VoteEvent("A".bind, id) followedBy MessageEvent(id, "A".bind) after 0.seconds
  println("Pattern3 is: " + p3)

//  p.map((e: Event) => { 0L } )
//    case ve: VoteEvent => 0L // somehow, Binding("TB").value - Binding("TA").value
//    case _ =>
//  })
}

//object EventStore {
//  implicit def longToTimestamp(l: Long): Timestamp = { new Timestamp(l) }
//
//  val EventStore = Set[Event](MessageEvent(0, 1, 0L), MessageEvent(2, 3, 1L), MessageEvent(1, 0, 2L))
//
//  def get: Set[Event] = { EventStore }
//}















