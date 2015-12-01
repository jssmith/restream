package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayMap}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

class ReplayMapImpl[K, V : ClassTag](default: => V) extends ReplayMap[K, V] with Threaded {
  val m = new java.util.concurrent.ConcurrentHashMap[K, ReplayValueImpl[V]]()
  override def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V] = {
    val x = m.get(key)
    if (x == null) {
      None
    } else {
      x.get(ts)(coordinator)
    }
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    ???
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit coordinator: CoordinatorInterface): Unit = {
    m.computeIfAbsent(key, new java.util.function.Function[K,ReplayValueImpl[V]] {
      override def apply(t: K): ReplayValueImpl[V] = {
        new ReplayValueImpl[V](default)
      }
    }).merge(ts, fn)(coordinator)
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
