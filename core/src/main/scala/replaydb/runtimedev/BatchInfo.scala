package replaydb.runtimedev

abstract class BatchInfo(val partitionId: Int, val phaseId: Int) {
  def batchEndTs: Long
}

object BatchInfo {
  implicit val defaultBatchInfo = new BatchInfo(0, 0) {
    override def batchEndTs: Long = throw new UnsupportedOperationException
  }
}