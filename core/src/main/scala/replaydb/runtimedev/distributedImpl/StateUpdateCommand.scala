package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateRead, StateWrite}
import replaydb.service.driver.Command

case class StateUpdateCommand(originatingWorkerId: Int, phaseId: Int, batchEndTs: Long, cmdsInBatch: Int,
                              writes: Array[Array[StateWrite[_]]], readPrepares: Array[Array[StateRead]]) extends Command