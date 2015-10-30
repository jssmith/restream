package replaydb.language.event

import replaydb.language.bindings.{TimeIntervalBinding, Binding}
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

class MessageEvent(val sendID: Binding[Long], val recvID: Binding[Long], _ts: TimeIntervalBinding) extends Event(_ts) {

  override def equals(a: Any): Boolean = {
    val other = a.asInstanceOf[MessageEvent]
    sendID == other.sendID && recvID == other.recvID && ts == other.ts
  }
  override def toString: String = {
    if (ts == null) {
      s"MessageEvent(from $sendID to $recvID)"
    } else {
      s"MessageEvent(from $sendID to $recvID at ($ts))"
    }
  }
}

object MessageEvent {
  def apply(sendID: Binding[Long], recvID: Binding[Long]): MessageEvent = {
    new MessageEvent(sendID, recvID, null)
  }
}