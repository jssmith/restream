package replaydb.runtimedev

import org.scalatest.FlatSpec
import replaydb.event.Event

class ReplayRuntimeSpec extends FlatSpec {
  "A ReplayRuntime" should "just compile" in {
    case class StopEvent(ts: Long) extends Event
    var endCt = 0L
    class Analysis {
      import ReplayRuntime._
      val c: ReplayCounter = new ReplayCounter {
        var ct = 0L
        var lastTs = Long.MinValue
        private def checkTimeIncrease(ts: Long): Unit = {
          if (ts < lastTs) {
            throw new RuntimeException("time decreased")
          }
          lastTs = ts
        }
        override def add(value: Long, ts: Long): Unit = {
          checkTimeIncrease(ts)
          ct += value
        }
        override def get(ts: Long): Long = {
          checkTimeIncrease(ts)
          ct
        }
      }

      bind { pv: ProductView => c.add(1, pv.ts) }
      bind { pv: ProductView => println(s"Have product view at time ${pv.ts}. Count is ${c.get(pv.ts)}.") }
      bind { se: StopEvent  => endCt = c.get(se.ts)}
      def update(x: Event) = emit(x)
    }

    val a = new Analysis
    a.update(new ProductUpdate(100L, 1L))
    a.update(new ProductView(100L, 2L))
    a.update(new ProductView(100L, 3L))
    a.update(new StopEvent(4L))
    assert(endCt === 2)
  }
}
