package replaydb.util

import java.lang.management.ManagementFactory
import java.util.concurrent.ThreadFactory

import scala.collection.mutable.ArrayBuffer

class TimingThreadFactory extends ThreadFactory {
  private class ThreadTiming(threadId: Long) {
    private var terminationValue: Option[Long] = None
    private def readThreadCpuTime(): Long = {
      ManagementFactory.getThreadMXBean.getThreadCpuTime(threadId)
    }
    def finish(): Unit = {
      this.synchronized {
        terminationValue = Some(readThreadCpuTime())
      }
    }
    def get(): Long = {
      this.synchronized {
        terminationValue match {
          case Some(t) => t
          case None =>
            readThreadCpuTime()
        }
      }
    }
  }

  private val threadTimes = new ArrayBuffer[ThreadTiming]()
  override def newThread(r: Runnable): Thread = {
    new Thread(new Runnable {
      override def run(): Unit = {
        val threadId = Thread.currentThread().getId
        val threadTiming = new ThreadTiming(threadId)
        threadTimes.synchronized {
          threadTimes += threadTiming
        }
        try {
          r.run()
        } finally {
          threadTiming.finish()
        }
      }
    })
  }

  def getSummary(): Long = {
    threadTimes.synchronized {
      threadTimes.map(_.get()).sum
    }
  }
}
