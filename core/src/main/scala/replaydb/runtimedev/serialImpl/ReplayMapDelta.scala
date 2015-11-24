package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayDelta, ReplayMap}

class ReplayMapDelta[K, V](parentMap: ReplayMapImpl[K, V]) extends ReplayMap[K, V] with ReplayDelta {

  override def get(ts: Long, key: K): Option[V] = {
    throw new UnsupportedOperationException
  }

  override def update(ts: Long, key: K, fn: V => V): Unit = {
    parentMap.update(ts, key, fn)
  }

  override def clear(): Unit = {
    // nothing to be done
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    ???
  }

}
