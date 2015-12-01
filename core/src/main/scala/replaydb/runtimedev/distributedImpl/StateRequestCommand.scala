package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.Command

// TODO is Command the correct thing to use? Or is that supposed to only be for the driver?
case class StateRequestCommand(collectionId: Int, ts: Long, key: Any) extends Command
