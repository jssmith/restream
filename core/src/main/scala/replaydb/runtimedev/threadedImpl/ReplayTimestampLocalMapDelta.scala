package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayTimestampLocalMap

import scala.collection.mutable

class ReplayTimestampLocalMapDelta[K, V]
  extends ReplayTimestampLocalMap[K, V] with ReplayDelta {

  case class Update(ts: Long, key: K, fn: V => V)

  var updates: Seq[Update] = Seq()

  override def update(ts: Long, key: K, fn: V => V): Unit = {
    this.synchronized {
      updates :+= Update(ts, key, fn)
    }
    // should be a parallel-friendly way to do this
  }

  override def clear(): Unit = {
    this.synchronized {
      updates = mutable.Seq()
    }
  }

  override def get(ts: Long, key: K): Option[V] = {
    throw new UnsupportedOperationException
  }

}
