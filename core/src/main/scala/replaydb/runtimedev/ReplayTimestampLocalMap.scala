package replaydb.runtimedev

import replaydb.runtimedev.CoordinatorInterface

trait ReplayTimestampLocalMap[K, V] extends ReplayState {

  def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V]
  def update(ts: Long, key: K, fn: (V) => V)(implicit coordinator: CoordinatorInterface): Unit

}
