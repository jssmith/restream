package replaydb.runtimedev.distributedImpl

import java.util.concurrent.ConcurrentHashMap

import replaydb.runtimedev.{ReplayMap, CoordinatorInterface}

import scala.reflect.ClassTag

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

  override def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V] = {
    val ret = preparedValues.get((ts, key))
    preparedValues.remove((ts, key))
    ret
  }

  override def getRandom(ts: Long): Option[(K, V)] = ???

  override def getPrepare(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Unit = {
    commService.localPrepareState(collectionId, ts, key, (key: K) => key.hashCode())
  }

  override def merge(ts: Long, key: K, fn: V => V)(implicit coordinator: CoordinatorInterface): Unit = {
    commService.submitWrite(collectionId, ts, key, fn, (key: K) => key.hashCode())
  }

  val preparedValues = new ConcurrentHashMap[(Long, K), Option[V]]()

  def insertPreparedValue(ts: Long, key: K, value: Option[V]): Unit = {
    preparedValues.put((ts, key), value)
  }


  val internalReplayMap = new replaydb.runtimedev.threadedImpl.ReplayMapImpl[K, V](default)

  def insertRemoteWrite(ts: Long, key: K, fn: V => V): Unit = {
    internalReplayMap.merge(ts, key, fn)
  }

  def requestRemoteRead(ts: Long, key: K): Option[V] = {
    internalReplayMap.get(ts, key)
  }

}
