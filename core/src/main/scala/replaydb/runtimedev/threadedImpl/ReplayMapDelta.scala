package replaydb.runtimedev.threadedImpl

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function

import scala.reflect.ClassTag


class ReplayMapDelta[K, V: ClassTag] {

  val m = new ConcurrentHashMap[K, ReplayValueDelta[V]]()

  def update(ts: Long, key: K, fn: (V) => V): Unit = {
    m.computeIfAbsent(key, new Function[K, ReplayValueDelta[V]] {
      override def apply(t: K): ReplayValueDelta[V] = {
        new ReplayValueDelta[V]
      }
    }).merge(ts, fn)
  }

}
