package replaydb.runtimedev.threadedImpl

import java.util.function.BiConsumer

import replaydb.runtimedev.{BatchInfo, ReplayTimestampLocalMap}

// NOTE: Should only be used with associative, commutative operators
class ReplayTimestampLocalMapImpl[K, V](default: => V) extends ReplayTimestampLocalMap[K, V] with Threaded {

  class ValueWithTimestamp(initialValue: V, val ts: Long) {
    var read = false
    var value = initialValue

    def updateValue(fn: (V) => V): Unit = {
      value = fn(value)
    }

  }

  val m = new java.util.concurrent.ConcurrentHashMap[K, ValueWithTimestamp]()
  override def get(ts: Long, key: K)(implicit batchInfo: BatchInfo): Option[V] = {
    val x = m.get(key)
    if (x == null) {
      None
    } else {
      if (x.ts == ts) {
        Option(x.value)
      } else {
        throw new IllegalArgumentException(s"Requested key $key at time $ts but $key is only valid at ${x.ts}")
      }
    }
  }

  override def getPrepare(ts: Long, key: K)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit batchInfo: BatchInfo): Unit = {
    val x = m.get(key)
    if (x == null) {
      m.put(key, new ValueWithTimestamp(fn(default), ts))
    } else if (x.read) {
      throw new IllegalStateException(s"Requested update on key $key but it was already read!")
    } else {
      x.updateValue(fn)
    }
  }

  // (total merged values in collection, total unmerged values, number of ReplayValues, GC'd values)
  override def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    var cnt = 0
    val oldSize = m.size
    m.forEach(new BiConsumer[K, ValueWithTimestamp] {
      override def accept(key: K, value: ValueWithTimestamp): Unit = {
        if (value.ts < ts) {
          m.remove(key)
          cnt += 1
        }
      }
    })
    (oldSize, 0, oldSize, cnt)
  }

}
