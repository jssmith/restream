package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{BatchInfo, ReplayMapTopK}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class ReplayMapTopKLazyImpl[K, V](default: V)(implicit ord: Ordering[V], ct: ClassTag[V])
  extends ReplayMapImpl[K, V](default) with ReplayMapTopK[K, V] {

  override def getTopK(ts: Long, count: Int)(implicit batchInfo: BatchInfo): TopK = {
    val pq = mutable.PriorityQueue[(K, V)]()(Ordering.by((_: (K, V))._2).reverse) // Sort by V only, reversed (lowest first)
    for ((k, replayV) <- m) {
      val optV = replayV.get(ts)
      optV match {
        case Some(v) =>
          if (pq.size < count) {
            pq.enqueue((k, v))
          } else if (ord.gt(v, pq.head._2)) {
            pq.dequeue()
            pq.enqueue((k, v))
          }
        case None =>
      }
    }
    pq.dequeueAll.toList.reverse
  }

  override def getPrepareTopK(ts: Long, k: Int)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }
}
