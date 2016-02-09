package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayCounter, ReplayMap, ReplayTimestampLocalMap}

import scala.reflect.ClassTag

class ReplayStateFactory extends replaydb.runtimedev.ReplayStateFactory {
  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V] = {
    new ReplayMapImpl[K, V](default)
  }

  def getReplayCounter: ReplayCounter = {
    new ReplayCounterImpl
  }

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V] = {
    new ReplayTimestampLocalMapImpl[K, V](default)
  }
}
