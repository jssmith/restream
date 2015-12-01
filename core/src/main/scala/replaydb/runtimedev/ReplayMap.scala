package replaydb.runtimedev

trait ReplayMap[K,V] extends ReplayState {
  def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V]
  def merge(ts: Long, key: K, fn: V => V)(implicit coordinator: CoordinatorInterface)
  def getRandom(ts: Long): Option[(K,V)]
}
