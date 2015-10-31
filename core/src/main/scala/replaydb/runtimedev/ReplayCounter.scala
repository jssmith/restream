package replaydb.runtimedev

trait ReplayCounter {
  def add(value: Long, ts: Long)
  def get(ts: Long): Long
}
