package replaydb.runtimedev.distributedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayValue}

class ReplayValueImpl[V](default: => V) extends ReplayValue[V] {

  override def get(ts: Long)(implicit coordinator: CoordinatorInterface): Option[V] = {
    ???
  }

  override def getPrepare(ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    ???
  }

  override def merge(ts: Long, fn: V => V)(implicit coordinator: CoordinatorInterface): Unit = {
    ???
  }

}
