package replaydb.service.driver

import replaydb.runtimedev.HasRuntimeInterface

class InitReplayCommand[T <: HasRuntimeInterface](val files: Map[Int, String],
                             program: Class[T],
                             val startTimestamp: Long,
                             val batchTimeInterval: Long,
                             val progressUpdateInterval: Int) extends Command {
  val programClass = program.getCanonicalName
}
