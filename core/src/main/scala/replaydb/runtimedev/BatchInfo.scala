package replaydb.runtimedev

abstract class BatchInfo(val partitionId: Int, val phaseId: Int) {
  def batchEndTs: Long
  def batchId: Int
}

object BatchInfo {
  implicit val defaultBatchInfo = new BatchInfo(0, 0) {
    override def batchEndTs: Long = throw new UnsupportedOperationException
    override def batchId: Int = throw new UnsupportedOperationException
  }
}