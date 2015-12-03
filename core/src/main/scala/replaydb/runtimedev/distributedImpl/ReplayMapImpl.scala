package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.{ReplayMap, CoordinatorInterface}

import scala.reflect.ClassTag
import scala.collection.mutable

class ReplayMapImpl[K, V : ClassTag](default: => V, collectionId: Int, commService: StateCommunicationService) extends ReplayMap[K, V] {

  /**
    * For each phase, store a separate internal collection?
    *
    *
    * there should be one collection that is sort of the *master* collection for this partition,
    *   holds whatever portion of the state this partition owns
    * merge function actually just adds the merge fn to a queue waiting to be sent out to correct partitions
    *
    * how does this collection know when it should communicate? we only want that on batch boundaries
    *
    * different collections should become aware of each other at some level to allow for maximum comm batching
    */

  class PrepValue(val value: Option[V]) {
    var count = 0
    def incrementCount(): Unit = count += 1
    def decrementCount(): Unit = count -= 1
  }

//  val preparedValues = new ConcurrentHashMap[(Long, K), Option[V]]()
  val preparedValues: mutable.Map[(Long, K), PrepValue] = mutable.Map()

  override def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V] = {
    preparedValues.synchronized {
      while (!preparedValues.contains((ts, key))) {
        preparedValues.wait()
      }
      val ret = preparedValues((ts, key))
      ret.decrementCount()
      if (ret.count == 0) {
        preparedValues.remove((ts, key))
      }
      ret.value
    }
  }

  override def getRandom(ts: Long): Option[(K, V)] = ???

  override def getPrepare(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Unit = {
    commService.localPrepareState(collectionId, coordinator.phaseId, coordinator.batchEndTs, ts, key, (key: K) => key.hashCode())
  }

  override def merge(ts: Long, key: K, fn: V => V)(implicit coordinator: CoordinatorInterface): Unit = {
    commService.submitWrite(collectionId, coordinator.phaseId, coordinator.batchEndTs, ts, key, fn, (key: K) => key.hashCode())
  }

  def insertPreparedValue(ts: Long, key: K, value: Option[V]): Unit = {
    preparedValues.synchronized {
      preparedValues.getOrElseUpdate((ts, key), new PrepValue(value)).incrementCount()
      preparedValues.notifyAll()
    }
  }


  val internalReplayMap = new replaydb.runtimedev.threadedImpl.ReplayMapImpl[K, V](default)

  def insertRemoteWrite(ts: Long, key: K, fn: V => V): Unit = {
    internalReplayMap.merge(ts, key, fn)
  }

  def requestRemoteRead(ts: Long, key: K): Option[V] = {
    internalReplayMap.get(ts, key)
  }

  // TODO remove
  def internalGc(ts: Long): (Int, Int, Int) = {
    internalReplayMap.internalGc(ts)
  }

  def gcOlderThan(ts: Long): Int = {
    internalReplayMap.gcOlderThan(ts)
  }
}
