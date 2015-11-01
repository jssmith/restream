package replaydb.runtimedev

trait ReplayCounter extends ReplayState {
  def add(value: Long, ts: Long)
  def get(ts: Long): Long
}
