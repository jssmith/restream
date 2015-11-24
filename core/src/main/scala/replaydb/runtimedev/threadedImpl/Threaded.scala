package replaydb.runtimedev.threadedImpl

trait Threaded {

  def gcOlderThan(ts: Long): Int
  def merge(delta: ReplayDelta): Unit
  def getDelta: ReplayDelta

}
