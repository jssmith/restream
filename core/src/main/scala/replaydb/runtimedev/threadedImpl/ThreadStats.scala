package replaydb.runtimedev.threadedImpl

import scala.collection.mutable

class ThreadStats(name: String) {
  val printThresholdDelta = 60
  var printThreshold: Long = printThresholdDelta
  class Stats() {

    var pq: mutable.PriorityQueue[Long] = mutable.PriorityQueue()
    var lastPrintCt = 0L

    def update(n: Long): Unit = {
      pq += n
      if (pq.size >= printThreshold) {
        printStats()
        printThreshold = pq.size + printThresholdDelta
        lastPrintCt = pq.size
      }
    }

    private def printStats(): Unit = {
      val quartileOne = pq.size/4*3
      val quartileTwo = pq.size/2
      val quartileThree = pq.size/4
      val max = 0
      var sum = 0L
      var qOne: Long = 0
      var qTwo: Long = 0
      var qThree: Long = 0
      var maxVal: Long = 0
      var i = 0
      for (v <- pq) {
        sum += v
        if (i == quartileOne) {
          qOne = v
        } else if (i == quartileTwo) {
          qTwo = v
        } else if (i == quartileThree) {
          qThree = v
        } else if (i == max) {
          maxVal = v
        }
        i += 1
      }
      println(s"$name:${Thread.currentThread().getName} - GC STATS: Quartiles: ($qOne, $qTwo, $qThree), Max: $maxVal, Average: ${sum/pq.size}")
    }
  }
  val tl = new  ThreadLocal[Stats]() {
    override protected def initialValue(): Stats = {
      new Stats
    }
  }
  def add(n: Long): Unit = {
    tl.get().update(n)
  }
}