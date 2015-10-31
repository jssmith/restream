package replaydb.runtimedev.monotonicImpl

import replaydb.runtimedev.ReplayCounter

class ReplayCounterImpl extends ReplayCounter with Monotonic {
  var ct = 0L
  override def add(value: Long, ts: Long): Unit = {
    checkTimeIncrease(ts)
    ct += value
  }
  override def get(ts: Long): Long = {
    checkTimeIncrease(ts)
    ct
  }
}
