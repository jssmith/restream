package replaydb.runtimedev.distributedImpl

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateResponse, StateRead, StateWrite}
import replaydb.runtimedev.{ReplayMap, BatchInfo}

import scala.reflect.ClassTag
import scala.collection.mutable

class ReplayMapImpl[K, V : ClassTag](default: => V, collectionId: Int, commService: StateCommunicationService,
                                     partitionFunction: K => List[Int] = null) extends ReplayMap[K, V] {

  val internalReplayMap = new replaydb.runtimedev.threadedImpl.ReplayMapImpl[K, V](default)

  val queuedWrites = new ConcurrentBufferQueue[StateWrite[_]]()
  val queuedLocalReadPrepares = new ConcurrentBufferQueue[StateRead]()
  val queuedRemoteReadPrepares = new ConcurrentBufferQueue[StateRead]()

  val preparedValues: Array[mutable.Map[Long, ConcurrentHashMap[(Long, K), Option[V]]]] = Array.ofDim(commService.numPhases)
  (0 until commService.numPhases).foreach(preparedValues(_) = mutable.HashMap())

  def prepareForBatch(phaseId: Int, batchEndTs: Long): Unit = {
    queuedWrites.prepForBatch(phaseId, batchEndTs)
    queuedLocalReadPrepares.prepForBatch(phaseId, batchEndTs)
    queuedRemoteReadPrepares.prepForBatch(phaseId, batchEndTs)
    preparedValues(phaseId)(batchEndTs) = new ConcurrentHashMap[(Long, K), Option[V]]()
  }

  /**
    * partitionFunction: Would normally just return a single-element list of a single integer,
    *                    generally the hash code of the key (or, if only a certain part of the
    *                    key matters for partitioning, a hash code of a specific part of the key).
    *                    If you want to store values on more than one partition, you can return multiple
    *                    different integers and it will be stored on all matching partitions. This can be
    *                    useful for e.g. a very read-heavy value (so far using it for UserPairs since they
    *                    are written infrequently and read frequently and only need to be stored in 2 places)
    */

  val partitionFn = if (partitionFunction == null) (key: K) => List(key.hashCode) else partitionFunction

  override def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V] = {
    preparedValues(batchInfo.phaseId)(batchInfo.batchEndTs).get((ts, key))
  }

  override def getRandom(ts: Long): Option[(K, V)] = ???

  override def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit = {
    val sourceWorker = commService.getSourceWorker(key, partitionFn)
    queuedLocalReadPrepares(batchInfo.phaseId, sourceWorker, batchInfo.batchEndTs).offer(StateRead(ts, key))
  }

  override def merge(ts: Long, key: K, fn: V => V)(implicit batchInfo: BatchInfo): Unit = {
    val destWorkers = commService.getDestWorkers(key, partitionFn)
    for (worker <- destWorkers) {
      queuedWrites(batchInfo.phaseId, worker, batchInfo.batchEndTs).offer(StateWrite(ts, key, fn))
    }
  }

  def getAndClearWrites(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateWrite[_]] = {
    queuedWrites.remove(phaseId, workerId, batchEndTs).toArray[StateWrite[_]](Array[StateWrite[_]]())
  }

  def getAndClearLocalReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateRead] = {
    queuedLocalReadPrepares.remove(phaseId, workerId, batchEndTs).toArray(Array[StateRead]())
  }

  def insertPreparedValues(phaseId: Int, batchEndTs: Long, responses: Array[StateResponse]): Unit = {
    for (resp <- responses) {
      preparedValues(phaseId+1)(batchEndTs).put((resp.ts, resp.key.asInstanceOf[K]), resp.value.asInstanceOf[Option[V]])
    }
  }

  def bufferRemoteReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long,
                               readPrepares: Array[StateRead]): Unit = {
    readPrepares.foreach(queuedRemoteReadPrepares(phaseId, workerId, batchEndTs).offer)
  }

  def fulfillRemoteReadPrepare(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateResponse] = {
    queuedRemoteReadPrepares.remove(phaseId, workerId, batchEndTs).toArray[StateRead](Array[StateRead]()).
      map(rp => StateResponse(rp.ts, rp.key, requestRemoteRead(rp.ts, rp.key.asInstanceOf[K])))
  }

  def insertRemoteWrites(writes: Array[StateWrite[_]]): Unit = {
    for (w <- writes) {
      internalReplayMap.merge(w.ts, w.key.asInstanceOf[K], w.asInstanceOf[StateWrite[V]].merge)
    }
  }

  private def requestRemoteRead(ts: Long, key: K): Option[V] = {
    internalReplayMap.get(ts, key)
  }

  def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    internalReplayMap.gcOlderThan(ts)
  }

  class ConcurrentBufferQueue[T]() {
    val queue: Array[Array[mutable.Map[Long, ConcurrentLinkedQueue[T]]]] =
      Array.ofDim(commService.numPhases, commService.numWorkers)

    for (i <- 0 until commService.numPhases; j <- 0 until commService.numWorkers) {
      queue(i)(j) = mutable.HashMap()
    }

    def apply(phaseId: Int, workerId: Int, batchEndTs: Long): ConcurrentLinkedQueue[T] = {
      get(phaseId, workerId, batchEndTs)
    }

    def get(phaseId: Int, workerId: Int, batchEndTs: Long): ConcurrentLinkedQueue[T] = {
      queue(phaseId)(workerId)(batchEndTs)
    }

    def remove(phaseId: Int, workerId: Int, batchEndTs: Long): ConcurrentLinkedQueue[T] = {
      queue(phaseId)(workerId).remove(batchEndTs).get
    }

    def prepForBatch(phaseId: Int, batchEndTs: Long): Unit = {
      for (j <- 0 until commService.numWorkers) {
        queue(phaseId)(j)(batchEndTs) = new ConcurrentLinkedQueue[T]()
      }
    }
  }
}
