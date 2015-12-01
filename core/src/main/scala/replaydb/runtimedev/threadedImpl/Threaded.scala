package replaydb.runtimedev.threadedImpl

trait Threaded {

  def gcOlderThan(ts: Long): Int

}
