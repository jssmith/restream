package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.{ReplayMap, BatchInfo}

import scala.reflect.ClassTag
import scala.collection.mutable

class ReplayMapImpl[K, V : ClassTag](default: => V, collectionId: Int, commService: StateCommunicationService,
                                     partitionFunction: K => List[Int] = null) extends ReplayMap[K, V] {

  /**
    * partitionFunction: Would normally just return a single-element list of a single integer,
    *                    generally the hash code of the key (or, if only a certain part of the
    *                    key matters for partitioning, a hash code of a specific part of the key).
    *                    If you want to store values on more than one partition, you can return multiple
    *                    different integers and it will be stored on all matching partitions. This can be
    *                    useful for e.g. a very read-heavy value (so far using it for UserPairs since they
    *                    are written infrequently and read frequently and only need to be stored in 2 places)
    */

  class PrepValue(val value: Option[V]) {
    var count = 0
    def incrementCount(): Unit = count += 1
    def decrementCount(): Unit = count -= 1
  }

//  val preparedValues = new ConcurrentHashMap[(Long, K), Option[V]]()
  val preparedValues: mutable.Map[(Long, K), PrepValue] = mutable.Map()
  val partitionFn = if (partitionFunction == null) (key: K) => List(key.hashCode) else partitionFunction

  override def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V] = {
    preparedValues.synchronized {
      while (!preparedValues.contains((ts, key)) || preparedValues((ts, key)).count == 0) {
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

  override def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit = {
    commService.localPrepareState(collectionId, batchInfo.phaseId, batchInfo.batchEndTs, ts, key, partitionFn)
  }

  override def merge(ts: Long, key: K, fn: V => V)(implicit batchInfo: BatchInfo): Unit = {
    commService.submitWrite(collectionId, batchInfo.phaseId, batchInfo.batchEndTs, ts, key, fn, partitionFn)
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

  def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    internalReplayMap.gcOlderThan(ts)
  }
}
