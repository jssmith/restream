package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.ReplayValue

import scala.collection.mutable

class ReplayValueImpl[T](default: => T) extends ReplayValue[T] with Serial {

  case class MergeRecord[S](ts: Long, merge: S => S) extends Ordered[MergeRecord[S]] {
    override def compare(that: MergeRecord[S]): Int = {
      that.ts.compareTo(ts)
    }
  }

  val outstanding: mutable.PriorityQueue[MergeRecord[T]] = mutable.PriorityQueue()
  var value = default

  override def merge(ts: Long, value: T => T): Unit = {
    outstanding.enqueue(new MergeRecord(ts, value))
  }

  override def getOption(ts: Long): Option[T] = {
    while (outstanding.nonEmpty && outstanding.head.ts <= ts) {
      value = outstanding.dequeue().merge(value)
    }
    Some(value)
  }

  val delta = new ReplayValueDelta[T](this)
  override def getDelta: ReplayValueDelta[T] = {
    delta
  }

}
