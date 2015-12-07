package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{BatchInfo, ReplayValue}

import scala.collection.mutable

class ReplayValueImpl[T](default: => T) extends ReplayValue[T] with Serial {

  case class MergeRecord[S](ts: Long, merge: S => S) extends Ordered[MergeRecord[S]] {
    override def compare(that: MergeRecord[S]): Int = {
      that.ts.compareTo(ts)
    }
  }

  val outstanding: mutable.PriorityQueue[MergeRecord[T]] = mutable.PriorityQueue()
  var value = default

  override def merge(ts: Long, value: T => T)(implicit batchInfo: BatchInfo): Unit = {
    outstanding.enqueue(new MergeRecord(ts, value))
  }

  override def get(ts: Long)(implicit batchInfo: BatchInfo): Option[T] = {
    while (outstanding.nonEmpty && outstanding.head.ts <= ts) {
      value = outstanding.dequeue().merge(value)
    }
    Some(value)
  }

  override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

}
