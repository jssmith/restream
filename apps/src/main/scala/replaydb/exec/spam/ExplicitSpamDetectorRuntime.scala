package replaydb.exec.spam

import replaydb.event.Event
import replaydb.runtimedev.{DistributedRuntimeInterface, ReplayState}

/**
 * This is code that would be generated by the compiler. Here
 * we generate it explicitly for testing purposes
 */
class ExplicitSpamDetectorRuntime extends DistributedRuntimeInterface {
  override def numPhases: Int = 4

  override def update(partitionId: Int, phase: Int, e: Event, deltaMap: Map[ReplayState, ReplayState]): Unit = {
    phase match {
      case 0 =>
      case 1 =>
      case 2 =>
      case 3 =>
    }
  }
}