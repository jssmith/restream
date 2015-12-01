package replaydb.runtimedev

import replaydb.runtimedev.CoordinatorInterface


trait ReplayCounter extends ReplayState {
  def add(value: Long, ts: Long)(implicit coordinator: CoordinatorInterface)
  def get(ts: Long)(implicit coordinator: CoordinatorInterface): Long
}
