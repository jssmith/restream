package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayMap}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

class ReplayMapImpl[K, V : ClassTag](default: => V) extends ReplayMap[K, V] with Threaded {
  val m = new java.util.concurrent.ConcurrentHashMap[K, ReplayValueImpl[V]]()
  override def get(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Option[V] = {
//    println(s"Requesting get on key $key within phase ${coordinator.phaseId}, partition ${coordinator.partitionId}, batch ${coordinator.batchId}")
    val x = m.get(key)
    if (x == null) {
      None
    } else {
      x.get(ts)(coordinator)
    }
  }

  def getPrepare(ts: Long, key: K)(implicit coordinator: CoordinatorInterface): Unit = {
    // Nothing to be done
//    println(s"Requesting getPrepare on key $key within phase ${coordinator.phaseId}, partition ${coordinator.partitionId}, batch ${coordinator.batchId}")
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

  // TODO remove
  def internalGc(ts: Long): (Int, Int, Int) = {
    var gcCount = 0
    var totalSize = 0
    for (entry <- m.values()) {
      val g = entry.gcWithCount(ts)
      totalSize += g._1
      gcCount += g._2
    }
    (m.size, totalSize, gcCount)
  }

  override def gcOlderThan(ts: Long): Int = {
    // TODO should be a more efficient way to do this. Considering the idea
    // of maintaining a set of values which have been modified since the last GC
    // but not sure if that will add unnecessary overhead
//    (for (entry <- m.values()) yield {
//      entry.gcOlderThan(ts)
//    }).sum
    internalGc(ts)._3
  }
}
