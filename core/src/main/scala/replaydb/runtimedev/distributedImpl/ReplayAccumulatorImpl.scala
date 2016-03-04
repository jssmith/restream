package replaydb.runtimedev.distributedImpl

import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.function.BiFunction

import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateRead, StateResponse, StateWrite}
import replaydb.runtimedev.{BatchInfo, ReplayAccumulator}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import collection.JavaConversions._

class ReplayAccumulatorImpl(collectionId: Int, commService: StateCommunicationService)
  extends ReplayAccumulator with Partitioned {

  val accumulated = new util.TreeMap[Long, Long]() // ts, value
  val accumulatedLock = new ReentrantReadWriteLock()
  val accumulatedEvaluated = new ConcurrentHashMap[Long, Long]()

  val queuedWrites: Array[mutable.Map[Long, ConcurrentHashMap[Long, Long]]] = Array.ofDim(commService.numPhases)
  val accumulatedWrites: Array[util.TreeMap[Long, util.TreeMap[Long, Long]]] = Array.ofDim(commService.numPhases)
  val accumulatedWriteLock = new ReentrantReadWriteLock()
  val queuedLocalReadPrepares: Array[mutable.Map[Long, ConcurrentLinkedQueue[StateRead]]] =
    Array.ofDim(commService.numPhases)
  val queuedRemoteReadPrepares: Array[Array[mutable.Map[Long, ArrayBuffer[StateRead]]]] =
    Array.ofDim(commService.numPhases, commService.numWorkers)

  val preparedValues: Array[mutable.Map[Long, ConcurrentHashMap[Long, Long]]] = 
    Array.ofDim(commService.numPhases)

  for (i <- 0 until commService.numPhases) {
    queuedWrites(i) = mutable.Map()
    accumulatedWrites(i) = new util.TreeMap()
    queuedLocalReadPrepares(i) = mutable.Map()
    preparedValues(i) = mutable.Map()

    for (j <- 0 until commService.numWorkers) {
      queuedRemoteReadPrepares(i)(j) = mutable.Map()
    }
  }

  def prepareForBatch(phaseId: Int, batchEndTs: Long): Unit = {
    queuedWrites(phaseId)(batchEndTs) = new ConcurrentHashMap()
    queuedLocalReadPrepares(phaseId)(batchEndTs) = new ConcurrentLinkedQueue[StateRead]()
    for (i <- 0 until commService.numWorkers) {
      queuedRemoteReadPrepares(phaseId)(i)(batchEndTs) = ArrayBuffer()
    }
    preparedValues(phaseId)(batchEndTs) = new ConcurrentHashMap()

    accumulatedLock.writeLock().lock()
    accumulated(batchEndTs) = 0
    accumulatedLock.writeLock().unlock()
  }

  def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo) = {
    queuedWrites(batchInfo.phaseId)(batchInfo.batchEndTs).merge(ts, value, new BiFunction[Long, Long, Long] {
      def apply(oldValue: Long, newValue: Long): Long = oldValue + value
    })
  }

  def get(ts: Long)(implicit batchInfo: BatchInfo): Long = {
    accumulatedEvaluated(batchInfo.batchEndTs) + preparedValues(batchInfo.phaseId)(batchInfo.batchEndTs).get(ts)
  }

  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    queuedLocalReadPrepares(batchInfo.phaseId)(batchInfo.batchEndTs).offer(StateRead(ts, 0))
  }

  def getAndClearWrites(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateWrite[_]] = {
    accumulatedWriteLock.readLock().lock()
    if (!accumulatedWrites(phaseId).contains(batchEndTs)) {
      val tempMap = new util.TreeMap[Long, Long](queuedWrites(phaseId)(batchEndTs))
      var runningSum = 0L
      val newMap = new util.TreeMap[Long, Long]()
      for ((k, v) <- tempMap) {
        runningSum += v
        newMap(k) = runningSum
      }
      accumulatedWriteLock.readLock().unlock()
      accumulatedWriteLock.writeLock().lock()
      accumulatedWrites(phaseId)(batchEndTs) = newMap
      accumulatedWriteLock.writeLock().unlock()
      accumulatedWriteLock.readLock().lock()
      queuedWrites(phaseId).remove(batchEndTs)
    }
    val value = if (accumulatedWrites(phaseId)(batchEndTs).isEmpty) { 0 } else {
      accumulatedWrites(phaseId)(batchEndTs).lastEntry.getValue
    }
    accumulatedWriteLock.readLock().unlock()
    Array(StateWrite[Long](batchEndTs, 0, _ => value))
  }

  def getAndClearLocalReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateRead] = {
    queuedLocalReadPrepares(phaseId)(batchEndTs).toArray(Array[StateRead]()).distinct
  }

  def insertPreparedValues(phaseId: Int, batchEndTs: Long, responses: Array[StateResponse]): Unit = {
    accumulatedLock.readLock().lock()
    accumulatedEvaluated.put(batchEndTs, accumulated.headMap(batchEndTs).values().sum)
    accumulatedLock.readLock().unlock()
    for (resp <- responses) {
      // could use ConcurrentHashMap.merge here but I think it's overkill
      preparedValues(phaseId + 1)(batchEndTs)(resp.ts) =
        preparedValues(phaseId + 1)(batchEndTs).getOrDefault(resp.ts, 0) + resp.value.get.asInstanceOf[Long]
    }
  }

  def bufferRemoteReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long,
                               readPrepares: Array[StateRead]): Unit = {
    readPrepares.foreach(queuedRemoteReadPrepares(phaseId)(workerId)(batchEndTs) += _)
  }

  def fulfillRemoteReadPrepare(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateResponse] = {
    accumulatedWriteLock.readLock().lock()
    val ret = (queuedRemoteReadPrepares(phaseId)(workerId).remove(batchEndTs) match {
      case Some(q) => q.toArray
      case None => Array()
    }).map(rp => StateResponse(rp.ts, rp.key, Some(requestRemoteRead(rp.ts, phaseId, batchEndTs))))
    accumulatedWriteLock.readLock().unlock()
    ret
  }

  def insertRemoteWrites(writes: Array[StateWrite[_]]): Unit = {
    accumulatedLock.writeLock().lock()
    for (w <- writes) {
      accumulated(w.ts) = accumulated(w.ts) + w.asInstanceOf[StateWrite[Long]].merge(0)
    }
    accumulatedLock.writeLock().unlock()
  }

  private def requestRemoteRead(ts: Long, phaseId: Int, batchEndTs: Long): Long = {
    accumulatedWrites(phaseId)(batchEndTs).floorEntry(ts) match {
      case null => 0
      case entry => entry.getValue
    }
  }

  def cleanupBatch(phaseId: Int, batchEndTs: Long): Unit = {
    preparedValues(phaseId).remove(batchEndTs)
    queuedLocalReadPrepares(phaseId).remove(batchEndTs)
  }

  def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    accumulatedLock.writeLock().lock()
    // traverse accumulated in order, roll the values up to the last one before ts
    // remove any old queuedWrite/queuedLocalReadPrepare buffers hanging around
    val oldSum = accumulated.headMap(ts, true).values().sum
    accumulated.headMap(ts).keySet().toArray.foreach(accumulated.remove(_))
    accumulated(ts) = oldSum
    accumulatedLock.writeLock().unlock()

    accumulatedWriteLock.writeLock().lock()
    for (aw <- accumulatedWrites) {
      aw.headMap(ts).keySet().toArray.foreach(aw.remove(_))
    }
    accumulatedWriteLock.writeLock().unlock()
    (-1, -1, -1, -1)
  }

}
