package replaydb.runtimedev

trait ReplayValue[T] extends ReplayState {
  def merge(ts: Long, value: T => T): Unit
  def getOption(ts: Long): Option[T]
}
