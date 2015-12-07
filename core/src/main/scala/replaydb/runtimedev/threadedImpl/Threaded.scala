package replaydb.runtimedev.threadedImpl

trait Threaded {

  // (total merged values in collection, total unmerged values, number of ReplayValues, GC'd values)
  def gcOlderThan(ts: Long): (Int, Int, Int, Int)

}
