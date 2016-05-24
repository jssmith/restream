package replaydb.runtimedev.causalLoggingImpl

import replaydb.runtimedev._

import scala.reflect.ClassTag

class ReplayStateFactory extends replaydb.runtimedev.ReplayStateFactory {

  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    new ReplayMapImpl[K, V](default)
  }
  def getReplayMapTopKLazy[K, V](default: => V, partitionFn: K => List[Int] = null)
                                (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V] = {
    ???
  }

  def getReplayMapTopK[K, V](default: => V, maxCount: Int, partitionFn: K => List[Int] = null)
                            (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V] = {
    ???
  }

  def getReplayCounter: ReplayCounter = {
    new ReplayCounterImpl
  }

  def getReplayAccumulator: ReplayAccumulator = {
    ???
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new serialImpl.ReplayTimestampLocalMapImpl[K, V](default)
  }
}
