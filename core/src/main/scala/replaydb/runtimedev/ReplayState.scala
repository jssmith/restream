package replaydb.runtimedev

trait ReplayState {
  def gcOlderThan(ts: Long): Int
}
