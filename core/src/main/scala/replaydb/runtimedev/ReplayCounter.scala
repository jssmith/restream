package replaydb.runtimedev

trait ReplayCounter extends ReplayState {
  def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo)
  def get(ts: Long)(implicit batchInfo: BatchInfo): Long
  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit
}
