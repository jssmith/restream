package replaydb.service.driver

import replaydb.runtimedev.HasRuntimeInterface

import scala.collection.mutable.ArrayBuffer

class InitReplayCommand[T <: HasRuntimeInterface](val workerId: Int,
                                                  val partitionMaps: ArrayBuffer[(Int, String)],
                                                  program: Class[T],
                                                  val runConfiguration: RunConfiguration)
  extends Command {
  val programClass = program.getCanonicalName
}
