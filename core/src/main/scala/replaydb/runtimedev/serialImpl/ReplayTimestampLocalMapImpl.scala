package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.ReplayTimestampLocalMap

import scala.collection.mutable


class ReplayTimestampLocalMapImpl[K, V](default: => V) extends ReplayTimestampLocalMap[K, V] with Serial {

  class ValueWithTimestamp(initialValue: V, val ts: Long) {
    var read = false
    var value = initialValue

    def updateValue(fn: (V) => V): Unit = {
      value = fn(value)
    }

  }

  val m = mutable.Map[K, ValueWithTimestamp]()

  override def get(ts: Long, key: K): Option[V] = {
    m.get(key) match {
      case Some(x) =>
        if (x.ts == ts) {
          Option(x.value)
        } else {
          throw new IllegalArgumentException(s"Requested key $key at time $ts but $key is only valid at ${x.ts}")
        }
      case None => None
    }
  }

  override def update(ts: Long, key: K, fn: (V) => V): Unit = {
    m.get(key) match {
      case Some(x) =>
        if (x.read) {
          throw new IllegalStateException(s"Requested update on key $key but it was already read!")
        } else {
          if (ts != x.ts) {
            throw new IllegalStateException(s"Requested update on key $key at time $ts but $key is only valid at ${x.ts}")
          }
          x.updateValue(fn)
        }
      case None =>
        m.put(key, new ValueWithTimestamp(fn(default), ts))
    }
  }

  val delta = new ReplayTimestampLocalMapDelta[K, V](this)
  override def getDelta: ReplayTimestampLocalMapDelta[K, V] = {
    delta
  }

  override def gcOlderThan(ts: Long): Int = {
    var cnt = 0
    m.foreach((entry: (K, ValueWithTimestamp)) => {
        if (entry._2.ts < ts) {
          m.remove(entry._1)
          cnt += 1
        }
      }
    )
    cnt
  }

}
