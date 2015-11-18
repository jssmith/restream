package replaydb.runtimedev

trait ReplayDelta extends ReplayState {

  def clear(): Unit

  override def getDelta: ReplayDelta = {
    throw new UnsupportedOperationException
  }

  override def merge(delta: ReplayDelta): Unit = {
    throw new UnsupportedOperationException
  }

  override def gcOlderThan(ts: Long): Int = {
    throw new UnsupportedOperationException
  }

}
