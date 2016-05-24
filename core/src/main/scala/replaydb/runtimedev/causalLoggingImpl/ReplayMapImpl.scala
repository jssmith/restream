package replaydb.runtimedev.causalLoggingImpl

import replaydb.runtimedev.{BatchInfo, ReplayMap, ReplayValue}

import scala.collection.mutable
import scala.util.Random

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K,V] with Serial with Logger {
  val m = mutable.Map[K, ReplayValue[V]]()

  override def getRandom(ts: Long): Option[(K, V)] = {
    if (m.nonEmpty) {
      val ma = m.toArray
      val chosen = ma(Random.nextInt(m.size))
      Some((chosen._1, chosen._2.get(ts).get))
    } else {
      None
    }
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit batchInfo: BatchInfo): Unit = {
    val replayVal = m.getOrElseUpdate(key, new ReplayValueImpl[V](default))
    replayVal.merge(ts, fn)(batchInfo)
  }

  override def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V] = {
    m.get(key) match {
      case Some(replayVal) => replayVal.get(ts)(batchInfo)
      case None => None
    }
  }

  override def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

}
