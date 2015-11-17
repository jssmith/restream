package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.threadedImpl.ReplayValueImpl.MergeRecord

import scala.collection.mutable
import scala.reflect.ClassTag

class ReplayValueDelta[T : ClassTag] {

  val pq: mutable.PriorityQueue[MergeRecord[T]] =
    new mutable.PriorityQueue()

  def merge(ts: Long, merge: T => T): Unit = {
    pq.enqueue(new MergeRecord[T](ts, merge))
  }

}
