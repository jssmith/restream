package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayCounter

import scala.collection.mutable.ArrayBuffer

class ReplayCounterImpl extends ReplayCounter {
  import replaydb.runtimedev.threadedImpl.ReplayCounterImpl._
  var lastAllocation: Allocation = allocate(null)
  override def add(value: Long, ts: Long): Unit = {
    if (!lastAllocation.canWrite) {
      lastAllocation = allocate(lastAllocation)
    }
    lastAllocation.write(ts, value)
  }

  override def get(ts: Long): Long = {
    lastAllocation.search(ts)
  }
}

object ReplayCounterImpl {
  val segmentSize = 1000000
  val allocMin = 2
  val allocMax = 10000

  class CounterStorage {
    val timestamps = new Array[Long](segmentSize)
    val values = new Array[Long](segmentSize)
    var pos = 0
    def alloc(n: Int): Int = {
      if (pos + n < segmentSize) {
        val ret = pos
        pos += n
        ret
      } else {
        -1
      }
    }
  }

  case class Allocation(parent: Allocation, epoch: Int, offset: Int, size: Int) {
    var usePoint = 0
    def canWrite: Boolean = {
      usePoint < size
    }
    def write(ts: Long, value: Long) = {
      storage(epoch).timestamps(offset + usePoint) = ts
      storage(epoch).values(offset + usePoint) = value
      usePoint += 1
    }
    def search(ts: Long): Long = {
      // Linear scan search - this isn't the most efficient
      // would like to do something like binary search on the
      // allocation known to contain the timestamp
/*
      var searchAllocation = this
      var searchEpochStorage = storage(epoch)
      var searchOffset = offset
      var searchPtr = offset + usePoint - 1
      var searchTs = searchEpochStorage.timestamps(searchPtr)
      while (searchTs > ts) {
        searchPtr -= 1
        if (searchPtr < searchOffset) {
          if (searchAllocation.parent != null) {
            searchAllocation = searchAllocation.parent
            searchEpochStorage = storage(searchAllocation.epoch)
            searchOffset = searchAllocation.offset
            searchPtr = searchAllocation.offset + searchAllocation.usePoint
          } else {
            return 0L
          }
        }
        searchTs = searchEpochStorage.timestamps(searchPtr)
      }
*/

      var searchAllocation = this
      var searchEpochStorage = storage(epoch)
      var searchOffset = offset
      var searchPtr = offset + usePoint
      var searchTs: Long = Long.MaxValue
      do {
        searchPtr -= 1
        if (searchPtr < searchOffset) {
          if (searchAllocation.parent != null) {
            searchAllocation = searchAllocation.parent
            searchEpochStorage = storage(searchAllocation.epoch)
            searchOffset = searchAllocation.offset
            searchPtr = searchAllocation.offset + searchAllocation.usePoint
          } else {
            return 0L
          }
        } else {
          searchTs = searchEpochStorage.timestamps(searchPtr)
        }
      } while (searchTs > ts)

      searchEpochStorage.values(searchPtr)
    }
  }

  val storage = ArrayBuffer[CounterStorage]()
  var storageAllocIndex = 0
  storage += new CounterStorage()

  def allocate(lastAlloc: Allocation): Allocation = {
    val allocSize = if (lastAlloc == null) {
      allocMin
    } else {
      if (lastAlloc.epoch == storageAllocIndex) {
        Math.max(2 * lastAlloc.size, allocMax)
      } else {
        allocMin
      }
    }
    val tryAllocOffset = storage(storageAllocIndex).alloc(allocSize)
    val allocOffset = if (tryAllocOffset == -1) {
      storageAllocIndex += 1
      storage += new CounterStorage()
      storage(storageAllocIndex).alloc(allocSize)
    } else {
      tryAllocOffset
    }
    new Allocation(lastAlloc, storageAllocIndex, allocOffset, allocSize)
  }

}