package replaydb.runtimedev

trait ReplayCounter extends ReplayState {
  def add(value: Long, ts: Long)(implicit coordinator: CoordinatorInterface)
  def get(ts: Long)(implicit coordinator: CoordinatorInterface): Long
  def getPrepare(ts: Long)(implicit coordinator: CoordinatorInterface): Unit
}
