package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.ReplayMap

import scala.reflect.ClassTag

class ReplayStateFactoryRandomPartition(commService: StateCommunicationService)
  extends replaydb.runtimedev.distributedImpl.ReplayStateFactory(commService) {

  override def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    val newMap = new ReplayMapImpl[K, V](default, nextStateId, commService, (key: K) => List(key.hashCode))
    commService.registerReplayState(nextStateId, newMap)
    nextStateId += 1
    newMap
  }

}
