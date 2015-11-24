package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayCounter, ReplayDelta}

class ReplayCounterDelta(parentCounter: ReplayCounter) extends ReplayCounter with ReplayDelta {

  override def add(value: Long, ts: Long): Unit = {
    parentCounter.add(value, ts)
  }

  override def get(ts: Long): Long = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    // nothing to be done
  }

}
