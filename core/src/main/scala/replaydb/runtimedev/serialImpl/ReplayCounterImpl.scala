package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayCounter}

class ReplayCounterImpl extends ReplayCounter with Serial {

  val replayVal = new ReplayValueImpl[Long](0)

  override def add(value: Long, ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    replayVal.merge(ts, _ + value)(coordinator)
  }

  override def get(ts: Long)(implicit coordinator: CoordinatorInterface): Long = {
    replayVal.get(ts)(coordinator).get
  }

  override def getPrepare(ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    // Nothing to be done
  }

}
