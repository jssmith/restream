package replaydb.util

import java.io.{FileInputStream, BufferedInputStream}
import replaydb.event.Event

import replaydb.io.SocialNetworkStorage

/**
 * We estimate the rate at which events are coming in for these files
 * by reading a small prefix from each one. So long as the event
 * spacing is reasonably uniform this is just fine.
 */
object EventRateEstimator {

  /**
   * 
   * @param startTime time at which the data starts
   * @param eventIntervalMs average interval, in milliseconds, between events
   */
  case class EventRateDescription(startTime: Long, eventIntervalMs: Double)
  
  private class Min {
    var min: Long = Long.MaxValue
    def update(x: Long): Unit = {
      min = Math.min(min, x)
    }
    def apply(): Long = min
  }

  private class Avg {
    var sum = 0L
    var ct = 0L
    def update(x: Long): Unit = {
      sum += x
      ct += 1
    }
    def apply(): Long = sum / ct
  }

  /**
   *
   * @param fnBase file name base
   * @param numPartitions number of partitions
   * @return [[EventRateDescription]] of the event rate
   */
  def estimateRate(fnBase: String, numPartitions: Int): EventRateDescription = {
    val s = new SocialNetworkStorage
    val eventsToRead = 10000
    val startTime = new Min()
    val avgRate = new Avg()
    for (n <- 0 until numPartitions) {
      var firstTime = Long.MaxValue
      var lastTime = Long.MinValue
      val is = new BufferedInputStream(new FileInputStream(s"$fnBase-$n"))
      try {
        s.readEvents(is = is,
          f = (e: Event) => {
            firstTime = Math.min(firstTime, e.ts)
            lastTime = Math.max(lastTime, e.ts)
          }, limit = eventsToRead)
      } finally {
        is.close()
      }
      startTime.update(firstTime)
      avgRate.update(lastTime - firstTime)
    }
    new EventRateDescription(startTime(), avgRate().toDouble / eventsToRead.toDouble)
  }

}
