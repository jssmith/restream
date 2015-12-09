package replaydb.runtimedev

trait HasReplayStates[T <: ReplayState] {
  def getAllReplayStates: Seq[T]
}
