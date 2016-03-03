package replaydb.service

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import replaydb.runtimedev.BatchInfo

import scala.collection.mutable

class BatchProgressCoordinator(startTimestamp: Long, batchTimeInterval: Long, partitionId: Int, numPartitions: Int, numPhases: Int) {
  val logger = Logger(LoggerFactory.getLogger(classOf[BatchProgressCoordinator]))
  abstract class ThreadCoordinator(partitionId: Int, phaseId: Int) extends BatchInfo(partitionId, phaseId) {
    def awaitAdvance(ts: Long): Unit
  }

  def tsToBatch(ts: Long): String = {
    s"$ts:B(${(ts - startTimestamp) / batchTimeInterval})"
  }

  def update(phaseId: Int, maxTimestamp: Long): Unit = {
    x(phaseId).foreach(_.update(maxTimestamp))
  }

  class PhaseLimit(phaseId: Int) extends ThreadCoordinator(partitionId, phaseId) {
    var maxTimestamp: Long = startTimestamp
    var currentBatchEndTs: Long = startTimestamp
    def update(maxTimestamp: Long): Unit = {
      logger.info(s"update limit at phase $phaseId to $maxTimestamp")
      this.synchronized {
        if (maxTimestamp > this.maxTimestamp) {
          this.maxTimestamp = maxTimestamp
          notifyAll()
        }
      }
    }
    def awaitAdvance(ts: Long): Unit = {
      this.synchronized {
        currentBatchEndTs = ts
        while (ts > maxTimestamp) {
          logger.info(s"awaiting advance for phase $phaseId, time requested ${tsToBatch(ts)}, time allowed ${tsToBatch(maxTimestamp)}")
          wait()
        }
//        logger.info(s"test condition(post): ${(if (phaseId == 1) ts - batchTimeInterval else ts) < maxTimestamp} $phaseId $ts $batchTimeInterval $ts")
        logger.info(s"advancing for phase $phaseId, time requested ${tsToBatch(ts)}, time allowed ${tsToBatch(maxTimestamp)}")
      }
    }

    def batchEndTs: Long = currentBatchEndTs
    def batchId: Int = throw new UnsupportedOperationException
  }

  val x: Array[Array[PhaseLimit]] = Array.ofDim(numPhases, numPartitions)
  for (i <- 0 until numPhases; j <- 0 until numPartitions) {
    x(i)(j) = new PhaseLimit(i)
  }

  def getCoordinator(partitionId: Int, phaseId: Int): ThreadCoordinator = {
    x(phaseId)(partitionId)
  }
}
