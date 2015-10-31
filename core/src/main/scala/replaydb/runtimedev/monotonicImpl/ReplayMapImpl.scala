package replaydb.runtimedev.monotonicImpl

import replaydb.runtimedev.ReplayMap

import scala.collection.mutable
import scala.util.Random

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K,V] with Monotonic {
  val m = mutable.Map[K,V]()
  override def put(key: K, value: V, ts: Long): Unit = {
    checkTimeIncrease(ts)
    m.put(key, value)
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    checkTimeIncrease(ts)
    if (m.nonEmpty) {
      val ma = m.toArray
      Some(ma(Random.nextInt(m.size)))
    } else {
      None
    }
  }

  override def update(key: K, fn: (V) => Unit, ts: Long): Unit = {
    checkTimeIncrease(ts)
    fn(m.getOrElseUpdate(key, default))
  }

  override def get(key: K, ts: Long): Option[V] = {
    checkTimeIncrease(ts)
    m.get(key)
  }
}
