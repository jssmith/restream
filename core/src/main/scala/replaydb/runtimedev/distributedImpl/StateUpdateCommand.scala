package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateRead, StateWrite}
import replaydb.service.driver.Command

case class StateUpdateCommand(originatingPartitionId: Int, phaseId: Int, batchEndTs: Long, cmdsInBatch: Int,
                              writes: Array[StateWrite[_]], readPrepares: Array[StateRead]) extends Command