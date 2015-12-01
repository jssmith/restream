package replaydb.runtimedev

trait ReplayTimestampLocalMap[K, V] extends ReplayState {

  def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V]
  def getPrepare(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Unit
  def merge(ts: Long, key: K, fn: (V) => V)(implicit coordinator: CoordinatorInterface): Unit

}
