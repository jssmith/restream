package replaydb.runtimedev

import replaydb.event.Event

trait RuntimeInterface {
  def numPhases: Int
  def update(phase: Int, e: Event): Unit
  def update(e: Event): Unit = {
    for (i <- 0 until numPhases) {
      update(i, e)
    }
  }
}
