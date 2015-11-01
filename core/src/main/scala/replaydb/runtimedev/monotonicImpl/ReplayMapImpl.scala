package replaydb.runtimedev.monotonicImpl

import replaydb.runtimedev.ReplayMap

import scala.collection.mutable
import scala.util.Random

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K,V] with Monotonic {
  val m = mutable.Map[K,V]()

  override def getRandom(ts: Long): Option[(K, V)] = {
    checkTimeIncrease(ts)
    if (m.nonEmpty) {
      val ma = m.toArray
      Some(ma(Random.nextInt(m.size)))
    } else {
      None
    }
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    checkTimeIncrease(ts)
    val newVal = fn(m.getOrElseUpdate(key, default))
    m.put(key, newVal)
  }

  override def get(ts: Long, key: K): Option[V] = {
    checkTimeIncrease(ts)
    m.get(key)
  }

}
