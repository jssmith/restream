package replaydb.runtimedev

trait ReplayValue[T] extends ReplayState {
  def merge(ts: Long, value: T => T)(implicit batchInfo: BatchInfo): Unit
  def get(ts: Long)(implicit batchInfo: BatchInfo): Option[T]
  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit
}
