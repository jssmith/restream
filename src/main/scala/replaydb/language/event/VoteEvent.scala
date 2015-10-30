package replaydb.language.event

import replaydb.language.bindings.Binding
import replaydb.language.time.Timestamp

class VoteEvent(val voterID: Binding[Long], val voteeID: Binding[Long], ts: Binding[Timestamp]) extends Event(ts) {
  override def equals(a: Any): Boolean = {
    val other = a.asInstanceOf[VoteEvent]
    voterID == other.voterID && voteeID == other.voteeID && ts == other.ts
  }
  override def toString: String = {
    if (ts == null) {
      s"VoteEvent(from $voterID to $voteeID)"
    } else {
      s"VoteEvent(from $voterID to $voteeID at ($ts))"
    }
  }
}

object VoteEvent {
  def apply(voterID: Binding[Long], voteeID: Binding[Long]): VoteEvent = {
    new VoteEvent(voterID, voterID, null)
  }
}