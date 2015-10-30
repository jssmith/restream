package replaydb.language.event

import replaydb.language.bindings.Binding
import replaydb.language.time.Timestamp

//class MessageEvent(val sendID: Long, val recvID: Long, ts: Timestamp) extends Event(ts) {
//  override def equals(a: Any): Boolean = {
//    val other = a.asInstanceOf[MessageEvent]
//    sendID == other.sendID && recvID == other.recvID && ts == other.ts
//  }
//  override def toString: String = {
//    s"MessageEvent(from $sendID to $recvID at $ts)"
//  }
//}

class MessageEvent(val sendID: Binding[Long], val recvID: Binding[Long], ts: Binding[Timestamp]) extends Event(ts) {
  override def equals(a: Any): Boolean = {
    val other = a.asInstanceOf[MessageEvent]
    sendID == other.sendID && recvID == other.recvID && ts == other.ts
  }
  override def toString: String = {
    s"MessageEvent(from $sendID to $recvID at ($ts))"
  }
}

// is this how this is supposed to work?
object MessageEvent {
  def apply(sendID: Long, recvID: Long, ts: Timestamp): MessageEvent = {
    new MessageEvent(sendID, recvID, ts)
  }
}