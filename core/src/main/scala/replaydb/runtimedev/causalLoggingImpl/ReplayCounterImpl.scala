package replaydb.runtimedev.causalLoggingImpl

import replaydb.runtimedev.{BatchInfo, ReplayCounter}

class ReplayCounterImpl extends ReplayCounter with Serial {

  val replayVal = new ReplayValueImpl[Long](0)

  override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    replayVal.merge(ts, _ + value)(batchInfo)
  }

  override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = {
    replayVal.get(ts)(batchInfo).get
  }

  override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

}
