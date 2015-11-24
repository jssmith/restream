package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayValue, ReplayMap}

import scala.collection.mutable
import scala.util.Random

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K,V] with Serial {
  val m = mutable.Map[K, ReplayValue[V]]()

  override def getRandom(ts: Long): Option[(K, V)] = {
    if (m.nonEmpty) {
      val ma = m.toArray
      val chosen = ma(Random.nextInt(m.size))
      Some((chosen._1, chosen._2.getOption(ts).get))
    } else {
      None
    }
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    val replayVal = m.getOrElseUpdate(key, new ReplayValueImpl[V](default))
    replayVal.merge(ts, fn)
  }

  override def get(ts: Long, key: K): Option[V] = {
    m.get(key) match {
      case Some(replayVal) => replayVal.getOption(ts)
      case None => None
    }
  }

}
