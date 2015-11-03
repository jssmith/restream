package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayMap, ReplayValue}

import scala.collection.mutable

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K, V] {
  val m = new java.util.concurrent.ConcurrentHashMap[K, ReplayValue[V]]()
  override def get(ts: Long, key: K): Option[V] = {
    val x = m.get(key)
    if (x == null) {
      None
    } else {
      x.getOption(ts)
    }
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    ???
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    m.computeIfAbsent(key, new java.util.function.Function[K,ReplayValue[V]] {
      override def apply(t: K): ReplayValue[V] = {
        new ReplayValueImpl[V](default)
      }
    }).merge(ts, fn)
  }
}
