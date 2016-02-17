package replaydb.runtimedev

trait ReplayCounter extends ReplayState {
  def increment(ts: Long)(implicit batchInfo: BatchInfo)
  def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo)
  def get(ts: Long)(implicit batchInfo: BatchInfo): Long
  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit
}

object ReplayCounter {
  def increment(i: Long): Long = i + 1
}