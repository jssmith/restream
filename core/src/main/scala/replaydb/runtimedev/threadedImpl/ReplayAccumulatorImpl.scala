package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayAccumulator, BatchInfo}

class ReplayAccumulatorImpl extends ReplayAccumulator with Threaded {

  val replayValue = new ReplayValueImpl[Long](0L)

  override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    replayValue.merge(ts, _ + value)(batchInfo)
  }
  override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = {
    replayValue.get(ts)(batchInfo) match {
      case Some(x) => x
      case None => 0
    }
  }

  override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

  override def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    replayValue.gcOlderThan(ts)
  }
}
