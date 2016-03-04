package replaydb.runtimedev

// TODO ETK currently only supports summation but should support
//          any commutative-associative operation
trait ReplayAccumulator extends ReplayState {
  def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo)
  def get(ts: Long)(implicit batchInfo: BatchInfo): Long
  def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit
}
