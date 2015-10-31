package replaydb.runtimedev

import org.scalatest.FlatSpec
import replaydb.event.Event
import replaydb.runtimedev.monotonicImpl._

class ReplayRuntimeSpec extends FlatSpec {
  "A ReplayRuntime" should "just compile" in {
    case class StopEvent(ts: Long) extends Event
    var endCt = 0L
    class Analysis {
      import ReplayRuntime._
      val c: ReplayCounter = new ReplayCounterImpl
      val cSku: ReplayMap[Long,ReplayCounter] = new ReplayMapImpl[Long, ReplayCounter](new ReplayCounterImpl)
      val allSkus: ReplayMap[Long,Int] = new ReplayMapImpl[Long,Int](0)

      bind { pu: ProductUpdate =>
        allSkus.put(pu.sku, 1, pu.ts)
      }
      bind { pv: ProductView =>
        c.add(1, pv.ts)
        cSku.update(1, _.add(1, pv.ts), pv.ts)
      }
      bind { pv: ProductView =>
        val ts = pv.ts
        val ct = c.get(ts)
        def printTraining(sku: Long, outcome: Boolean) = {
          val featureValue = if (ct > 0) {
            0D
          } else {
            cSku.get(sku, ts).orNull.get(ts).toDouble / ct.toDouble
          }
          println(s"$outcome,$featureValue")
        }
        printTraining(pv.sku, outcome = true)
        printTraining(allSkus.getRandom(ts).orNull._1, outcome = false)
      }
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
