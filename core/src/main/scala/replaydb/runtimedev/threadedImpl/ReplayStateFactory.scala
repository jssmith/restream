package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev._

import scala.reflect.ClassTag

class ReplayStateFactory extends replaydb.runtimedev.ReplayStateFactory {
  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    new ReplayMapImpl[K, V](default)
  }

  def getReplayMapTopK[K, V](default: => V, partitionFn: K => List[Int] = null)
                            (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V] = {
    new ReplayMapTopKImpl[K, V](default)(ord, ct)
  }

  def getReplayCounter: ReplayCounter = {
    new ReplayCounterImpl
  }

  def getReplayAccumulator: ReplayAccumulator = {
    new ReplayAccumulatorImpl
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new ReplayTimestampLocalMapImpl[K, V](default)
  }
}
