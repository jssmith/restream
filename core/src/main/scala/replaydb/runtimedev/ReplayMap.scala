package replaydb.runtimedev

trait ReplayMap[K,V] {
  def get(ts: Long, key: K): Option[V]
  def update(ts: Long, key: K, fn: V => V)
  def getRandom(ts: Long): Option[(K,V)]
}
