package replaydb.runtimedev.threadedImpl

class ThreadAvg(name: String) {
  val printThresholdDelta = 100000
  var printThreshold: Long = printThresholdDelta
  class Avg (var ct: Long, var sum: Long) {
    var lastPrintCt = 0L
    var lastPrintSum = 0L
    def update(n: Long): Unit = {
      sum += n
      ct += 1
      if (sum >= printThreshold) {
        val deltaSum = sum - lastPrintSum
        val deltaCt = ct - lastPrintCt
        println(s"$name:${Thread.currentThread().getName} - ($sum/$ct=${sum.toDouble / ct.toDouble})  - ($deltaSum/$deltaCt=${deltaSum.toDouble/deltaCt.toDouble})")
        printThreshold = (sum / printThresholdDelta + 1) * printThresholdDelta
        lastPrintCt = ct
        lastPrintSum = sum
      }
    }
  }
  val tl = new  ThreadLocal[Avg]() {
    override protected def initialValue(): Avg = {
      new Avg(0, 0)
    }
  }
  def add(n: Int): Unit = {
    tl.get().update(n)
  }
}
