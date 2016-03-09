package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.threadedImpl.ReplayTimestampLocalMapImpl
import replaydb.runtimedev._

import scala.reflect.ClassTag

class ReplayStateFactory(commService: StateCommunicationService) extends replaydb.runtimedev.ReplayStateFactory {

  var nextStateId = 0

  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    val newMap = new ReplayMapImpl[K, V](default, nextStateId, commService, partitionFn)
    commService.registerReplayState(nextStateId, newMap)
    nextStateId += 1
    newMap
  }

  def getReplayMapTopKLazy[K, V](default: => V, partitionFn: K => List[Int] = null)
                                (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V] = {
    val newMap = new ReplayMapTopKLazyImpl[K, V](default, nextStateId, commService, partitionFn)(ord, ct)
    commService.registerReplayState(nextStateId, newMap)
    nextStateId += 1
    newMap
  }

  def getReplayMapTopK[K, V](default: => V, maxCount: Int, partitionFn: K => List[Int] = null)
                            (implicit ord: Ordering[V], ct: ClassTag[V]): ReplayMapTopK[K, V] = {
    val newMap = new ReplayMapTopKImpl[K, V](default, maxCount, nextStateId, commService, partitionFn)(ord, ct)
    commService.registerReplayState(nextStateId, newMap)
    nextStateId += 1
    newMap
  }

  def getReplayCounter: ReplayCounter = {
    val newCounter = new ReplayCounterImpl(nextStateId, commService)
    commService.registerReplayState(nextStateId, newCounter.internalMap)
    nextStateId += 1
    newCounter
  }

  def getReplayAccumulator: ReplayAccumulator = {
    val newAccumulator = new ReplayAccumulatorImpl(nextStateId, commService)
    commService.registerReplayState(nextStateId, newAccumulator)
    nextStateId += 1
    newAccumulator
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new ReplayTimestampLocalMapImpl[K, V](default)
  }
}
