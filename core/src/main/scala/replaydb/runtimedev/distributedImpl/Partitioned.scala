package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateRead, StateResponse, StateWrite}

trait Partitioned {
  def prepareForBatch(phaseId: Int, batchEndTs: Long): Unit

  def getAndClearWrites(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateWrite[_]]

  def getAndClearLocalReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateRead]

  def insertPreparedValues(phaseId: Int, batchEndTs: Long, responses: Array[StateResponse]): Unit

  def bufferRemoteReadPrepares(phaseId: Int, workerId: Int, batchEndTs: Long,
                               readPrepares: Array[StateRead]): Unit

  def fulfillRemoteReadPrepare(phaseId: Int, workerId: Int, batchEndTs: Long): Array[StateResponse]

  def insertRemoteWrites(writes: Array[StateWrite[_]]): Unit

  def cleanupBatch(phaseId: Int, batchEndTs: Long): Unit

  def gcOlderThan(ts: Long): (Int, Int, Int, Int)
}
