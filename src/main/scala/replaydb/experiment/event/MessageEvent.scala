package replaydb.experiment.event

import replaydb.experiment.Timestamp

/**
 * Created by erik on 10/27/15.
 */
class MessageEvent(val sendID: Long, val recvID: Long, ts: Timestamp) extends Event(ts) {
  override def equals(a: Any): Boolean = {
    val other = a.asInstanceOf[MessageEvent]
    sendID == other.sendID && recvID == other.recvID && ts == other.ts
  }
  override def toString: String = {
    s"MessageEvent(from $sendID to $recvID at $ts)"
  }
}

// is this how this is supposed to work?
object MessageEvent {
  def apply(sendID: Long, recvID: Long, ts: Timestamp): MessageEvent = {
    new MessageEvent(sendID, recvID, ts)
  }
}