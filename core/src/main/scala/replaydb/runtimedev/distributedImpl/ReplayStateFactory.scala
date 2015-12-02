package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.threadedImpl.ReplayTimestampLocalMapImpl
import replaydb.runtimedev.{ReplayCounter, ReplayMap, ReplayTimestampLocalMap}

import scala.reflect.ClassTag

class ReplayStateFactory(commService: StateCommunicationService) extends replaydb.runtimedev.ReplayStateFactory {

  var nextStateId = 0

  def getReplayMap[K, V : ClassTag](default: => V): ReplayMap[K, V] = {
    val newMap = new ReplayMapImpl[K, V](default, nextStateId, commService)
    commService.registerReplayState(nextStateId, newMap)
    nextStateId += 1
    newMap
  }

  def getReplayCounter: ReplayCounter = {
    // TODO there is no distributedImpl of ReplayCounter yet
    throw new UnsupportedOperationException
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new ReplayTimestampLocalMapImpl[K, V](default)
  }
}
