package replaydb.runtimedev.distributedImpl

import replaydb.service.driver.Command

case class StateWriteCommand[T](collectionId: Int, ts: Long, key: Any, merge: T => T) extends Command