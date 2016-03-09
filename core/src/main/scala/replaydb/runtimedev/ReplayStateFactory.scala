package replaydb.runtimedev

import scala.reflect.ClassTag

trait ReplayStateFactory {

  // See distributedImpl.ReplayMapImpl for a description of partitionFn
  // Note that this only applies to distributedImpl.ReplayStateFactory
  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V]

  def getReplayMapTopKLazy[K, V](default: => V, partitionFn: K => List[Int] = null)
                                (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V]

  def getReplayMapTopK[K, V](default: => V, maxCount: Int, partitionFn: K => List[Int] = null)
                            (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V]

  def getReplayCounter: ReplayCounter

  def getReplayAccumulator: ReplayAccumulator

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V]
}
