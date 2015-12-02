package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.Command

case class StateRequestCommand(collectionId: Int, ts: Long, key: Any) extends Command
