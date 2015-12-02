package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.distributedImpl.StateCommunicationService.StateResponse
import replaydb.service.driver.Command

case class StateRequestResponse(phaseId: Int, batchEndTs: Long, responses: Array[StateResponse]) extends Command
