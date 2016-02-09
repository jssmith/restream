package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.threadedImpl.ReplayTimestampLocalMapImpl
import replaydb.runtimedev.{ReplayCounter, ReplayMap, ReplayTimestampLocalMap}

import scala.reflect.ClassTag

class ReplayStateFactory(commService: StateCommunicationService) extends replaydb.runtimedev.ReplayStateFactory {

  var nextStateId = 0

  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    val newMap = new ReplayMapImpl[K, V](default, nextStateId, commService, partitionFn)
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

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new ReplayTimestampLocalMapImpl[K, V](default)
  }
}
