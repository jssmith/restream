package replaydb.runtimedev.threadedImpl

class ReplayCounterDelta extends ReplayValueDelta[Long] {

  def add(value: Long, ts: Long): Unit = {
    merge(ts, _ + value)
  }

}
