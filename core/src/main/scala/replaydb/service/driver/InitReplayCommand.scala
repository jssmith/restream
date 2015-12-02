package replaydb.service.driver

import replaydb.runtimedev.HasRuntimeInterface

class InitReplayCommand[T <: HasRuntimeInterface](val files: Map[Int, String],
                             program: Class[T],
                             val hostId: Int,
                             val runConfiguration: RunConfiguration) extends Command {
  val programClass = program.getCanonicalName
}
