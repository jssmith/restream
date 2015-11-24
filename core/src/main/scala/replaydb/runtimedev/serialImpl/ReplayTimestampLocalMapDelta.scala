package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayTimestampLocalMap, ReplayDelta}

class ReplayTimestampLocalMapDelta[K, V](parentMap: ReplayTimestampLocalMapImpl[K, V])
  extends ReplayTimestampLocalMap[K, V] with ReplayDelta {

  override def get(ts: Long, key: K): Option[V] = {
    parentMap.get(ts, key)
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    parentMap.update(ts, key, fn)
  }

  override def clear(): Unit = {
    // nothing to be done
  }

}
