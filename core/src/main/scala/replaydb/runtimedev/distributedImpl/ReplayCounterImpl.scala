package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.{BatchInfo, ReplayCounter}

class ReplayCounterImpl(collectionId: Int, commService: StateCommunicationService) extends ReplayCounter {

  // Simply use a ReplayMap with a single value to store this, just to keep from having to
  // reimplement similar functionality. Use the collectionId as the key so that different
  // ReplayCounters will end up located in different places
  val internalMap = new ReplayMapImpl[Int, Long](0L, collectionId, commService)

  def increment(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    internalMap.merge(ts, collectionId, ReplayCounter.increment)
  }

  def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    internalMap.merge(ts, collectionId, _ + value)
  }

  def get(ts: Long)(implicit batchInfo: BatchInfo): Long = {
    internalMap.get(ts, collectionId) match {
      case Some(v) => v
      case None => 0
    }
  }

  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    internalMap.getPrepare(ts, collectionId)
  }
}
