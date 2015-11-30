package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayValue
import replaydb.runtimedev.threadedImpl.ReplayValueImpl.MergeRecord

import scala.collection.mutable
import scala.reflect.ClassTag

class ReplayValueDelta[T : ClassTag]
  extends ReplayValue[T] with ReplayDelta {

  val pq: mutable.PriorityQueue[MergeRecord[T]] =
    new mutable.PriorityQueue()

  override def merge(ts: Long, merge: T => T): Unit = {
    pq.enqueue(new MergeRecord[T](ts, merge))
  }

  override def clear(): Unit = {
    pq.clear()
  }

  override def get(ts: Long): Option[T] = {
    throw new UnsupportedOperationException
  }

}
