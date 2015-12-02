package replaydb.runtimedev

abstract class CoordinatorInterface(val partitionId: Int, val phaseId: Int) {
  def reportCheckpoint(ts: Long, ct: Long): (Long, Long)

  def reportFinished(): Unit

  def gcAllReplayState(): Unit

  // TODO should only have one of these two
  def batchId: Int
  def batchEndTs: Long
}

object CoordinatorInterface {
  implicit val defaultCoordinatorInterface: CoordinatorInterface = new CoordinatorInterface(0, 0) {
    override def gcAllReplayState(): Unit = throw new UnsupportedOperationException
    override def batchId: Int = throw new UnsupportedOperationException
    override def batchEndTs: Long = throw new UnsupportedOperationException
    override def reportCheckpoint(ts: Long, ct: Long): (Long, Long) = throw new UnsupportedOperationException
    override def reportFinished(): Unit = throw new UnsupportedOperationException
  }
}