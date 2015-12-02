package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.Command

case class StateRequestResponse(collectionId: Int, ts: Long, key: Any, value: Any) extends Command
