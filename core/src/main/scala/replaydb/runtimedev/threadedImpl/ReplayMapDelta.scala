package replaydb.runtimedev.threadedImpl

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import replaydb.runtimedev.ReplayMap

import scala.reflect.ClassTag


class ReplayMapDelta[K, V: ClassTag]
  extends ReplayMap[K, V] with ReplayDelta {

  val m = new ConcurrentHashMap[K, ReplayValueDelta[V]]()

  override def merge(ts: Long, key: K, fn: (V) => V): Unit = {
    m.computeIfAbsent(key, new Function[K, ReplayValueDelta[V]] {
      override def apply(t: K): ReplayValueDelta[V] = {
        new ReplayValueDelta[V]
      }
    }).merge(ts, fn)
  }

  override def clear(): Unit = {
    m.clear()
  }

  override def get(ts: Long, key: K): Option[V] = {
    throw new UnsupportedOperationException
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    throw new UnsupportedOperationException
  }

}
