package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayCounter


class ReplayCounterImpl extends ReplayValueImpl[Long](0L) with ReplayCounter {
  override def add(value: Long, ts: Long): Unit = {
    merge(ts, _ + value)
  }
  override def get(ts: Long): Long = {
    getOption(ts) match {
      case Some(x) => x
      case None => 0
    }
  }
}
