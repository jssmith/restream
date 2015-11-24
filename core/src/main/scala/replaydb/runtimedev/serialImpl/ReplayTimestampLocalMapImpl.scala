package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.ReplayTimestampLocalMap

import scala.collection.mutable


class ReplayTimestampLocalMapImpl[K, V](default: => V) extends ReplayTimestampLocalMap[K, V] with Serial {

  var currentTs: Long = _
  var currentMap: Map[K, V] = Map()

  override def get(ts: Long, key: K): Option[V] = {
    if (ts == currentTs) {
      currentMap.get(key)
    } else {
      None
    }
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    if (ts != currentTs) {
      currentTs = ts
      currentMap = Map((key, fn(default)))
    } else {
      val current = currentMap.getOrElse(key, default)
      currentMap += ((key, fn(current)))
    }
  }

}
