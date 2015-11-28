package replaydb.service.driver

case class ProgressUpdateCommand (partitionId: Int, phaseId: Int,
                                  numProcessed: Int, finishedTimestamp: Long,
                                  done: Boolean) extends Command
