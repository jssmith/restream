package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayTimestampLocalMap}


class ReplayTimestampLocalMapImpl[K, V](default: => V) extends ReplayTimestampLocalMap[K, V] with Serial {

  var currentTs: Long = _
  var currentMap: Map[K, V] = Map()

  override def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V] = {
    if (ts == currentTs) {
      currentMap.get(key)
    } else {
      None
    }
  }

  override def getPrepare(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Unit = {
    // Nothing to be done
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit coordinator: CoordinatorInterface): Unit = {
    if (ts != currentTs) {
      currentTs = ts
      currentMap = Map((key, fn(default)))
    } else {
      val current = currentMap.getOrElse(key, default)
      currentMap += ((key, fn(current)))
    }
  }

}
