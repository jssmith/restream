package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayDelta, ReplayCounter}

class ReplayCounterDelta extends ReplayCounter with ReplayDelta {

  val replayDelta = new ReplayValueDelta[Long]

  override def add(value: Long, ts: Long): Unit = {
    replayDelta.merge(ts, _ + value)
  }

  override def clear(): Unit = {
    replayDelta.clear()
  }

  override def get(ts: Long): Long = {
    throw new UnsupportedOperationException
  }

}
