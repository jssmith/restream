package replaydb.runtimedev

import replaydb.event.Event

trait RuntimeInterface {
  def numPhases: Int
  def update(phase: Int, e: Event)(implicit coordinator: CoordinatorInterface): Unit
  def update(e: Event)(implicit coordinator: CoordinatorInterface): Unit = {
    update(coordinator.phaseId, e)(coordinator)
  }
  def updateAllPhases(e: Event)(implicit coordinator: CoordinatorInterface): Unit = {
    for (i <- 1 to numPhases) {
      update(i, e)(coordinator)
    }
  }


//  def update(partitionId: Int, phase: Int, e: Event, deltaMap: Map[ReplayState, ReplayState]): Unit
//  def update(partitionId: Int, e: Event, deltaMap: Map[ReplayState, ReplayState]): Unit = {
//    for (i <- 1 to numPhases) {
//      update(partitionId, i, e, deltaMap)
//    }
//  }
}
