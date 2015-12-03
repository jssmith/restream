package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayValue}
import replaydb.runtimedev.threadedImpl.ReplayValueImpl.MergeRecord

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class ReplayValueImpl[T : ClassTag](default: => T) extends ReplayValue[T] with Threaded {

  private case class ValueRecord[S](ts: Long, value: S) extends Ordered[ValueRecord[S]] {
    override def compare(that: ValueRecord[S]): Int = {
      that.ts.compareTo(ts)
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
  private var validDataStart = 0
  private var validDataEnd = 0
  def size = validDataEnd - validDataStart

  private val updates = new mutable.PriorityQueue[MergeRecord[T]]()
  private var lastRead = Long.MinValue
  private var lastGC = Long.MinValue
  private var oldestNonGCWrite = Long.MinValue

  // TODO we should be able to use ReadWrite locks here, right?

  override def merge(ts: Long, merge: T => T)(implicit coordinator: CoordinatorInterface): Unit = {
    this.synchronized {
      if (ts <= lastRead || ts <= lastGC) {
        throw new IllegalArgumentException(s"add at $ts must precede get at $lastRead and GC at $lastGC")
      }

      oldestNonGCWrite = if (oldestNonGCWrite < lastGC) {
        // If we're the first write since the last GC,  make ourselves the new oldest
        ts
      } else {
        // if we're older than another write following the last GC, update ourselves as oldest
        Math.min(oldestNonGCWrite, ts)
      }
      updates.enqueue(new MergeRecord[T](ts = ts, merge = merge))
    }
  }

  override def get(ts: Long)(implicit coordinator: CoordinatorInterface): Option[T] = {
    this.synchronized {
      lastRead = math.max(lastRead, ts)
      if (updates.nonEmpty) {
        val value = mergeUpdates(ts)
        if (value.nonEmpty) {
          return value
        }
      } else {
        ReplayValueImpl.mergeAvg.add(0)
      }
      return findValue(ts)._2
    }
  }

  override def getPrepare(ts: Long)(implicit coordinator: CoordinatorInterface): Unit = {
    // Nothing to be done
  }

  // Search for the most recent value less than or equal to ts
  // return (index, Some(value)) where index is the location of
  // value in the values() array if found, else return (0, None)
  private def findValue(ts: Long): (Int, Option[T]) = {
    // Naive linear search implementation
    var i = validDataEnd - 1
    while (i >= validDataStart) {
      if (timestamps(i) <= ts) {
        ReplayValueImpl.hitAvg.add(validDataEnd - i + 1)
        return (i, Some(values(i)))
      }
      i -= 1
    }
    ReplayValueImpl.missAvg.add(size)
    (0, None)
  }

  // Merge all outstanding updates up to ts, and possibly
  // return the last value before/equal to ts (*only* if that value
  // is also the most recent (non-merged) value) - this method
  // returning None does not signify there is no applicable value,
  // but does *guarantee* that the returned value is located at
  // values(validDataEnd - 1)
  private def mergeUpdates(ts: Long): Option[T] = {
    val toMerge = ArrayBuffer[MergeRecord[T]]()
    while (updates.nonEmpty && updates.head.ts <= ts) {
      toMerge += updates.dequeue()
    }
    ReplayValueImpl.mergeAvg.add(toMerge.size)
    if (toMerge.nonEmpty) {
      // Ensure space available
      resizeValueArrayIfNecessary(toMerge.length)
      var lastValue: T = if (size > 0) values(validDataEnd - 1) else default
      toMerge.foreach { mr =>
        lastValue = mr.merge(lastValue)
        timestamps(validDataEnd) = mr.ts
        values(validDataEnd) = lastValue
        validDataEnd += 1
      }
      ReplayValueImpl.hitAvg.add(0)
      // Just merged, so answer is at the top
      Some(values(validDataEnd - 1))
    } else {
      None
    }
  }

  // If the values and timestamps arrays aren't large enough to
  // hold their current size plus mergeSize, resize them
  // this may actually *decrease* the size of the arrays if
  // validDataStart is a significant fraction of values.length
  private def resizeValueArrayIfNecessary(mergeSize: Int): Unit = {
    if (timestamps == null) {
      val n = nextPowerOf2(mergeSize)
      timestamps = new Array[Long](n)
      values = new Array[T](n)
    } else {
      if (validDataEnd + mergeSize >= timestamps.length) {
        val newSize = nextPowerOf2(size + mergeSize)
        val newTimestamps = new Array[Long](newSize)
        val newValues = new Array[T](newSize)

        System.arraycopy(timestamps, validDataStart, newTimestamps, 0, size)
        System.arraycopy(values, validDataStart, newValues, 0, size)
        validDataEnd = size
        validDataStart = 0
        timestamps = newTimestamps
        values = newValues
      }
    }
  }

  // TODO remove
  def gcWithCount(ts: Long): (Int, Int) = {
    (size, gcOlderThan(ts))
  }

  // returns the number of items collected
  def gcOlderThan(ts: Long): Int = {
    if (ts < oldestNonGCWrite) {
      return 0 // Nothing to be done:
    }
    var valuesCollected: Int = 0
    this.synchronized {
      lastGC = ts
      val lastValue = mergeUpdates(ts)
      lastValue match {
        case Some(value) =>
          // garbage collected everything except one value
          values(0) = value
          timestamps(0) = timestamps(validDataEnd - 1)
          for (i <- 1 until validDataEnd) {
            values(i) = null.asInstanceOf[T] // Give JVM a chance to GC objects stored in here
            // timestamps(i) = 0L // TODO this shouldn't be necessary since Longs should be stored as primitive, no need for GC hints
          }
          valuesCollected = size - 1
          validDataStart = 0
          validDataEnd = 1
          // TODO Consider the possibility of doing some array size reduction here?
          // instead of null-ing out old array, could just create a new (smaller) array and put
          // the relevant value as the first element of that
          // but maybe not necessary, we already resize the array down if it gets too full
        case None =>
          val (index, value) = findValue(ts)
          if (value.isEmpty) {
            return 0 // No values older than ts, nothing to garbage collect
          }
          for (i <- validDataStart until index) {
            values(i) = null.asInstanceOf[T] // Give JVM a chance to GC objects
          }
          valuesCollected = index - validDataStart
          validDataStart = index
      }
    }
    valuesCollected
  }
}

object ReplayValueImpl {
  case class MergeRecord[T](ts: Long, merge: T => T) extends Ordered[MergeRecord[T]] {
    override def compare(that: MergeRecord[T]): Int = {
      that.ts.compareTo(ts)
    }
  }

  val mergeAvg = new ThreadAvg("ValueMerge")
  val missAvg = new ThreadAvg("ValueMiss")
  val hitAvg = new ThreadAvg("ValueHit")
  val gcAvg = new ThreadAvg("GC-Collected")
}