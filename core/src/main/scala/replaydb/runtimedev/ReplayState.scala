package replaydb.runtimedev

trait ReplayState {
  def gcOlderThan(ts: Long): Int
  def merge(delta: ReplayDelta): Unit
  def getDelta: ReplayDelta
}
