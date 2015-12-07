package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.BatchInfo

abstract class CoordinatorInterface(partitionId: Int, phaseId: Int) extends BatchInfo(partitionId, phaseId) {
  def reportCheckpoint(ts: Long, ct: Long): (Long, Long)

  def reportFinished(): Unit

  def gcAllReplayState(): Unit

  override def batchEndTs: Long = throw new UnsupportedOperationException
}