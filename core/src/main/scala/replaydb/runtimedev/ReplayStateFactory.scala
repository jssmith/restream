package replaydb.runtimedev

import scala.reflect.ClassTag

trait ReplayStateFactory {

  // See distributedImpl.ReplayMapImpl for a description of partitionFn
  // Note that this only applies to distributedImpl.ReplayStateFactory
  def getReplayMap[K, V : ClassTag](default: => V, partitionFn: K => List[Int] = null): ReplayMap[K, V]

  def getReplayCounter: ReplayCounter

  def getReplayTimestampLocalMap[K, V](default: => V): ReplayTimestampLocalMap[K, V]
}
