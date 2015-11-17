package replaydb.runtimedev

trait ReplayTimestampLocalMap[K, V] extends ReplayState {

  def get(ts: Long, key: K): Option[V]
  def update(ts: Long, key: K, fn: (V) => V): Unit

}
