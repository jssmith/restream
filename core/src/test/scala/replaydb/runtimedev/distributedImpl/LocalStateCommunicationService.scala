package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.{Command, RunConfiguration}

class LocalStateCommunicationService(workerId: Int, runConfig: RunConfiguration)
  extends StateCommunicationService(workerId, 1, runConfig) {

  override def connectToClients(): Unit = {}
  override def close(): Unit = {}

  var workers: List[LocalStateCommunicationService] = List()
  def setWorkers(w: List[LocalStateCommunicationService]): Unit = {
    workers = w
  }

  override def issueCommandToWorker(wId: Int, cmd: Command): Unit = {
    cmd match {
      case c: StateUpdateCommand => workers(wId).handleStateUpdateCommand(c)
      case c: StateRequestResponse => workers(wId).handleStateRequestResponse(c)
    }
  }
}
