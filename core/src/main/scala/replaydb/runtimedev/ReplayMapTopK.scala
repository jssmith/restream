package replaydb.runtimedev

trait ReplayMapTopK[K, V] extends ReplayMap[K, V] {
  type TopK = List[(K, V)]

  def getTopK(ts: Long, k: Int)(implicit batchInfo: BatchInfo): TopK
  def getPrepareTopK(ts: Long, k: Int)(implicit batchInfo: BatchInfo): Unit
}
