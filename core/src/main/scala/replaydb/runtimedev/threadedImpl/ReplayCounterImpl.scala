package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayCounter}

class ReplayCounterImpl extends ReplayCounter with Threaded {

  val replayValue = new ReplayValueImpl[Long](0L)

  override def add(value: Long, ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    replayValue.merge(ts, _ + value)(coordinator)
  }

  override def get(ts: Long)(implicit coordinator: CoordinatorInterface): Long = {
    replayValue.get(ts)(coordinator) match {
      case Some(x) => x
      case None => 0
    }
  }

  override def getPrepare(ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    // Nothing to be done
  }

  override def gcOlderThan(ts: Long): Int = {
    replayValue.gcOlderThan(ts)
  }

}
