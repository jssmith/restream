package replaydb.service.driver

import replaydb.runtimedev.HasRuntimeInterface

class InitReplayCommand[T <: HasRuntimeInterface](
                             val partitionId: Int,
                             val filename: String,
                             program: Class[T],
                             val runConfiguration: RunConfiguration,
                             val printProgressInterval: Int) extends Command {
  val programClass = program.getCanonicalName
}
