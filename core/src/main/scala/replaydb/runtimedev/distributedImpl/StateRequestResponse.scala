package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.Command

// TODO should this be a "Command"?
case class StateRequestResponse(collectionId: Int, ts: Long, key: Any, value: Any) extends Command
