package replaydb.runtimedev

trait ReplayValue[T] {
  def merge(ts: Long, value: T => T): Unit
  def getOption(ts: Long): Option[T]
}
