package replaydb.runtimedev

abstract class CoordinatorInterface(val partitionId: Int, val phaseId: Int) {
  def reportCheckpoint(ts: Long, ct: Long): (Long, Long)

  def reportFinished(): Unit

  def gcAllReplayState(): Unit

  def batchId: Int
}

object CoordinatorInterface {
  implicit val defaultCoordinatorInterface: CoordinatorInterface = null
}