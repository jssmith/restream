package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayCounter, ReplayMap, ReplayTimestampLocalMap, serialImpl}

import scala.reflect.ClassTag

class ReplayStateFactory extends replaydb.runtimedev.ReplayStateFactory {
  val useParallel = false

  def getReplayMap[K, V : ClassTag](default: => V): ReplayMap[K, V] = {
    new serialImpl.ReplayMapImpl[K, V](default)
  }

  def getReplayCounter: ReplayCounter = {
    new serialImpl.ReplayCounterImpl
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new serialImpl.ReplayTimestampLocalMapImpl[K, V](default)
  }
}
