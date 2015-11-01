package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayMap, ReplayValue}

import scala.collection.mutable

class ReplayMapImpl[K, V](default: => V) extends ReplayMap[K, V] {
  val m = mutable.HashMap[K, ReplayValue[V]]()
  override def get(ts: Long, key: K): Option[V] = {
    m.get(key) match {
      case Some(x) => x.getOption(ts)
      case None => None
    }
  }

  override def getRandom(ts: Long): Option[(K, V)] = {
    ???
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    m.getOrElseUpdate(key, new ReplayValueImpl[V](default)).merge(ts, fn)
  }
}
