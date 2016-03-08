package replaydb.runtimedev.distributedImpl

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import replaydb.runtimedev.distributedImpl.ReplayMapTopKImpl.{StateResponseTopK, StateReadTopK}
import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateResponse, StateRead}
import replaydb.runtimedev.{BatchInfo, ReplayMapTopK}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

class ReplayMapTopKImpl[K, V](default: V, collectionId: Int,
                                         commService: StateCommunicationService,
                                         partitionFunction: K => List[Int] = null)
                                        (implicit ord: Ordering[V], ct: ClassTag[V])
  extends ReplayMapImpl[K, V](default, collectionId, commService) with ReplayMapTopK[K, V] {

  override val internalReplayMap = new replaydb.runtimedev.threadedImpl.ReplayMapTopKImpl[K, V](default)
  val preparedTopKValues: Array[ConcurrentHashMap[Long, ConcurrentHashMap[Long, TopK]]] = Array.ofDim(commService.numPhases)
  (0 until commService.numPhases).foreach(preparedTopKValues(_) =
    new ConcurrentHashMap[Long, ConcurrentHashMap[Long, TopK]]())

  override def prepareForBatch(phaseId: Int, batchEndTs: Long): Unit = {
    super.prepareForBatch(phaseId, batchEndTs)
    preparedTopKValues(phaseId)(batchEndTs) = new ConcurrentHashMap[Long, TopK]()
  }

  override def getTopK(ts: Long, count: Int)(implicit batchInfo: BatchInfo): TopK = {
    preparedTopKValues(batchInfo.phaseId)(batchInfo.batchEndTs)(ts)
  }

  override def getPrepareTopK(ts: Long, k: Int)(implicit batchInfo: BatchInfo): Unit = {
    for (i <- 0 until commService.numWorkers) {
      queuedLocalReadPrepares(batchInfo.phaseId, i, batchInfo.batchEndTs).offer(StateReadTopK(ts, k))
    }
  }

  override def insertPreparedValues(phaseId: Int, batchEndTs: Long, responses: Array[StateResponse]): Unit = {
    super.insertPreparedValues(phaseId, batchEndTs, responses)

    for (resp <- responses.filter(_.isInstanceOf[StateResponseTopK[K,V]])) {
      val r = resp.asInstanceOf[StateResponseTopK[K,V]]
      val respTopK = r.value.toList
      preparedTopKValues(phaseId + 1)(batchEndTs).merge(r.ts, respTopK.sortWith((a,b) => ord.gt(a._2,b._2)),
        new BiFunction[TopK, TopK, TopK] {
        def apply(oldValue: TopK, newValue: TopK): TopK = {
          var idxO = 0
          var idxN = 0
          var outList: TopK = List()
          while ((idxO < oldValue.length || idxN < newValue.length) && (outList.length < r.k)) {
            if (idxO == oldValue.length || (idxN < newValue.length && ord.gt(newValue(idxN)._2, oldValue(idxO)._2))) {
              outList :+= newValue(idxN)
              idxN += 1
            } else {
              outList :+= oldValue(idxO)
              idxO += 1
            }
          }
          outList
        }
      })
    }

  }

  override def fulfillRemoteReadPrepare(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateResponse] = {
    val responses = queuedRemoteReadPrepares(phaseId, workerId, batchEndTs)
      .filter(_.isInstanceOf[StateReadTopK]).map(_.asInstanceOf[StateReadTopK]).toArray
      .map(rp => StateResponseTopK(rp.ts, rp.k, requestRemoteTopKRead(rp.ts, rp.k).toArray))

//    queuedRemoteReadPrepares(phaseId, workerId, batchEndTs).removeIf(new Predicate[StateRead] {
//      override def test(t: StateRead): Boolean = t.isInstanceOf[StateReadMap]
//    })
    responses ++ super.fulfillRemoteReadPrepare(phaseId, workerId, batchEndTs)
  }

  def requestRemoteTopKRead(ts: Long, k: Int): TopK = {
    internalReplayMap.getTopK(ts, k)
  }

  override def cleanupBatch(phaseId: Int, batchEndTs: Long): Unit = {
    preparedTopKValues(phaseId).remove(batchEndTs)
  }
}

object ReplayMapTopKImpl {
  case class StateReadTopK(ts: Long, k: Int) extends StateRead
  case class StateResponseTopK[K, V](ts: Long, k: Int, value: Array[(K, V)]) extends StateResponse
}
