package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{BatchInfo, ReplayMap}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

class ReplayMapImpl[K, V : ClassTag](default: => V) extends ReplayMap[K, V] with Threaded {
  val m = new java.util.concurrent.ConcurrentHashMap[K, ReplayValueImpl[V]]()
  override def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V] = {
//    println(s"Requesting get on key $key within phase ${coordinator.phaseId}, partition ${coordinator.partitionId}, batch ${coordinator.batchId}")
    val x = m.get(key)
    if (x == null) {
      None
    } else {
      x.get(ts)(batchInfo)
    }
  }

  def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
//    println(s"Requesting getPrepare on key $key within phase ${coordinator.phaseId}, partition ${coordinator.partitionId}, batch ${coordinator.batchId}")
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    ???
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit batchInfo: BatchInfo): Unit = {
    m.computeIfAbsent(key, new java.util.function.Function[K,ReplayValueImpl[V]] {
      override def apply(t: K): ReplayValueImpl[V] = {
        new ReplayValueImpl[V](default)
      }
    }).merge(ts, fn)(batchInfo)
  }

  override def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    // TODO should be a more efficient way to do this? Considering the idea
    // of maintaining a set of values which have been modified since the last GC
    // but not sure if that will add unnecessary overhead
    (for (entry <- m.values()) yield {
      entry.gcOlderThan(ts)
    }).reduceOption((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)) match {
      case Some(x) => x
      case _ => (0, 0, 0, 0)
    }
  }
}
