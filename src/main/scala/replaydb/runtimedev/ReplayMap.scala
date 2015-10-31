package replaydb.runtimedev

trait ReplayMap[K,V] {
  def put(key: K, value: V, ts: Long)
  def get(key: K, ts: Long): Option[V]
  def update(key: K, fn: V => Unit, ts: Long)
  def getRandom(ts: Long): Option[(K,V)]
}
