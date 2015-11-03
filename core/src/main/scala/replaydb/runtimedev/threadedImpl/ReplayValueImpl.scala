package replaydb.runtimedev.threadedImpl

import java.util.PriorityQueue

import replaydb.runtimedev.ReplayValue

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class ReplayValueImpl[T : ClassTag](default: => T) extends ReplayValue[T] {

  private case class ValueRecord[T](ts: Long, value: T) extends Ordered[ValueRecord[T]] {
    override def compare(that: ValueRecord[T]): Int = {
      ts.compareTo(that.ts)
    }
  }
  private case class MergeRecord[T](ts: Long, merge: T => T) extends Ordered[MergeRecord[T]] {
    override def compare(that: MergeRecord[T]): Int = {
      ts.compareTo(that.ts)
    }
  }

  private def nextPowerOf2(x: Int): Int = {
    // See: https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    var v = x
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v + 1
  }

  private var timestamps: Array[Long] = null
  private var values: Array[T] = null
  private var size = 0

  private val updates = new PriorityQueue[MergeRecord[T]]()
  private var lastRead = Long.MinValue

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
          val mergeSize = toMerge.length
          // Ensure space available
          if (timestamps == null) {
            val n = nextPowerOf2(mergeSize)
            timestamps = new Array[Long](n)
            values = new Array[T](n)
          } else {
            if (size + mergeSize >= timestamps.length) {
              val newSize = nextPowerOf2(size + mergeSize)
              val newTimestamps = new Array[Long](newSize)
              val newValues = new Array[T](newSize)
              System.arraycopy(timestamps, 0, newTimestamps, 0, values.length)
              System.arraycopy(values, 0, newValues, 0, values.length)
              timestamps = newTimestamps
              values = newValues
            }
          }
          var lastValue: T = if (size > 0) {
            values(size - 1)
          } else {
            default
          }
          toMerge.foreach { mr =>
            lastValue = mr.merge(lastValue)
            timestamps(size) = mr.ts
            values(size) = lastValue
            size += 1
          }
          // Just merged, so answer is at the top
          ReplayValueImpl.hitAvg.add(0)
          return Some(values(size-1))
        }
      } else {
        ReplayValueImpl.mergeAvg.add(0)
      }
      // Naive linear search implementation
      var i = size - 1
      while (i >= 0) {
        if (timestamps(i) <= ts) {
          ReplayValueImpl.hitAvg.add(size - i + 1)
          return Some(values(i))
        }
        i -= 1
      }
      ReplayValueImpl.missAvg.add(size)
      None
    }
  }
}

object ReplayValueImpl {
  val mergeAvg = new ThreadAvg("ValueMerge")
  val missAvg = new ThreadAvg("ValueMiss")
  val hitAvg = new ThreadAvg("ValueHit")
}