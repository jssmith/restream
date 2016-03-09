package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.threadedImpl.Threaded
import replaydb.runtimedev.{ReplayMap, ReplayMapTopK, BatchInfo}

import scala.reflect.ClassTag

class ReplayMapTopKImpl[K, V](default: V, maxCount: Int, collectionId: Int,
                              commService: StateCommunicationService,
                              partitionFunction: K => List[Int] = null)
                             (implicit ord: Ordering[V], ct: ClassTag[V])
  extends ReplayMapTopKLazyImpl[K, V](default, collectionId, commService)(ord, ct) {

  override val internalReplayMapTopK: ReplayMapTopK[K, V] with Threaded =
    new replaydb.runtimedev.threadedImpl.ReplayMapTopKImpl[K, V](default, maxCount)
  override val internalReplayMap: ReplayMap[K, V] with Threaded = internalReplayMapTopK

  // Restrict count to maxCount or else the process of combining responses from different
  // partitions could end up returning more than maxCount (and could be incorrect)
  override def getPrepareTopK(ts: Long, count: Int)(implicit batchInfo: BatchInfo): Unit = {
    super.getPrepareTopK(ts, Math.min(count, maxCount))
  }
}