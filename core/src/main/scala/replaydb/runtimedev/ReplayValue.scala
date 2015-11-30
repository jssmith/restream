package replaydb.runtimedev

trait ReplayValue[T] extends ReplayState {
  def merge(ts: Long, value: T => T): Unit
  def get(ts: Long): Option[T]
//  def prepareGet(ts: Long): Unit
}
