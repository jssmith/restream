package replaydb.runtimedev.distributedImpl

import scala.collection.mutable

class StateCommunicationService(workerId: Int, workerCount: Int) {

  // State is owned by worker #: partitionFn(key) % workerCount

  // Send a request to the machine which owns this key
  def localPrepareState[K, V](collectionId: Int, ts: Long, key: K, partitionFn: K => Int): Unit = {
    val srcWorker = partitionFn(key) % workerCount
    if (srcWorker == workerId) {
      val rs = states(collectionId).asInstanceOf[ReplayMapImpl[K, V]]
      rs.insertPreparedValue(ts, key, rs.get(ts, key))
    } else {
      val cmd = new StateRequestCommand(collectionId, ts, key)
      // send cmd across network to partition srcWorker
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, ts: Long, key: K, merge: V => V, partitionFn: K => Int): Unit = {
    val destWorker = partitionFn(key) % workerCount
    if (destWorker == workerId) {
      states(collectionId).asInstanceOf[ReplayMapImpl[K, V]].insertRemoteWrite(ts, key, merge)
    } else {
      val cmd = StateWriteCommand(collectionId, ts, key, merge)
      // send cmd across network to partition destWorker
    }
  }

  val states: mutable.Map[Int, ReplayMapImpl[_, _]] = mutable.HashMap()

  def registerReplayState(collectionId: Int, rs: ReplayMapImpl[_, _]): Unit = {
    if (states.contains(collectionId)) {
      throw new IllegalStateException("Can't have two collections with the same ID")
    }
    states += collectionId -> rs
  }

}
