package replaydb.runtimedev.threadedImpl

import java.util.PriorityQueue

import replaydb.runtimedev.ReplayValue

import scala.collection.mutable.ArrayBuffer


class ReplayValueImpl[T](default: => T) extends ReplayValue[T] {

  case class ValueRecord[T](ts: Long, value: T) extends Ordered[ValueRecord[T]] {
    override def compare(that: ValueRecord[T]): Int = {
      ts.compareTo(that.ts)
    }
  }
  case class MergeRecord[T](ts: Long, merge: T => T) extends Ordered[MergeRecord[T]] {
    override def compare(that: MergeRecord[T]): Int = {
      ts.compareTo(that.ts)
    }
  }

  val updates = new PriorityQueue[MergeRecord[T]]()
  var history: ArrayBuffer[ValueRecord[T]] = null
  var lastRead = Long.MinValue

  override def merge(ts: Long, merge: T => T): Unit = {
    this.synchronized {
      if (ts <= lastRead) {
        throw new IllegalArgumentException(s"add at $ts must follow get at $lastRead")
      }
      updates.add(new MergeRecord[T](ts = ts, merge = merge))
    }
  }

  override def getOption(ts: Long): Option[T] = {
    this.synchronized {
      lastRead = math.max(lastRead, ts)
      if (updates.size > 0) {
        val toMerge = ArrayBuffer[MergeRecord[T]]()
        while (updates.size() > 0 && updates.peek().ts <= ts) {
          toMerge += updates.poll()
        }
        ReplayValueImpl.mergeAvg.add(toMerge.size)
        if (toMerge.nonEmpty) {
          val cumSum: ValueRecord[T] = if (history != null && history.nonEmpty) {
            history.last
          } else {
            new ValueRecord(Long.MinValue, default)
          }
          val sortedUpdates = toMerge.scanLeft(cumSum)((a: ValueRecord[T], b: MergeRecord[T]) => new ValueRecord(b.ts, b.merge(a.value)))
          if (history != null) {
            history ++= sortedUpdates
          } else {
            history = new ArrayBuffer[ValueRecord[T]] ++ sortedUpdates
          }
        }
      } else {
        ReplayValueImpl.mergeAvg.add(0)
      }
      var checkedCt = 0
      if (history != null) {
        for (cr <- history.reverseIterator) {
          checkedCt += 1
          if (cr.ts <= ts) {
            ReplayValueImpl.hitAvg.add(checkedCt)
            return Some(cr.value)
          }
        }
      }
      ReplayValueImpl.missAvg.add(checkedCt)
      None
    }
  }
}

object ReplayValueImpl {
  val mergeAvg = new ThreadAvg("ValueMerge")
  val missAvg = new ThreadAvg("ValueMiss")
  val hitAvg = new ThreadAvg("ValueHit")
}