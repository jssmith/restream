package replaydb.runtimedev

trait ReplayMap[K,V] extends ReplayState {
  def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V]
  def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit
  def merge(ts: Long, key: K, fn: V => V)(implicit batchInfo: BatchInfo)
  def getRandom(ts: Long): Option[(K,V)]
}
