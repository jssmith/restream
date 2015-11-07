package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayMap, ReplayValue}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

class ReplayMapImpl[K, V : ClassTag](default: => V) extends ReplayMap[K, V] {
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

  override def gcOlderThan(ts: Long): Int = {
    // TODO should be a more efficient way to do this. Considering the idea
    // of maintaining a set of values which have been modified since the last GC
    // but not sure if that will add unnecessary overhead
    (for (entry <- m.values()) yield {
      entry.gcOlderThan(ts)
    }).sum
  }
}
