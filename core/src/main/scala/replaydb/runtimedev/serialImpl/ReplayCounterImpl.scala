package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.ReplayCounter

class ReplayCounterImpl extends ReplayCounter with Serial {

  val replayVal = new ReplayValueImpl[Long](0)

  override def add(value: Long, ts: Long): Unit = {
    replayVal.merge(ts, _ + value)
  }

  override def get(ts: Long): Long = {
    replayVal.get(ts).get
  }

}
