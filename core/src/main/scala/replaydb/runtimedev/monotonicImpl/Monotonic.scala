package replaydb.runtimedev.monotonicImpl

protected trait Monotonic {
  var lastTs = Long.MinValue
  protected def checkTimeIncrease(ts: Long): Unit = {
    if (ts < lastTs) {
      throw new RuntimeException("time decreased")
    }
    lastTs = ts
  }
}
