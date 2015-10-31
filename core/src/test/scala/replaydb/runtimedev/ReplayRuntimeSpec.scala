package replaydb.runtimedev

import org.scalatest.FlatSpec
import replaydb.event.{MessageEvent, Event}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev.monotonicImpl._

class ReplayRuntimeSpec extends FlatSpec {
  "A ReplayRuntime" should "implement overall popularity" in {
    case class StopEvent(ts: Long) extends Event
    var endCt = 0L
    class Analysis {
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
    a.update(new ProductUpdate(ts = 1L, sku = 100L))
    a.update(new ProductView(ts = 2L, sku = 100L))
    a.update(new ProductView(ts = 3L, sku = 100L))
    a.update(new StopEvent(ts = 4L))
    assert(endCt === 2)
  }

  case class PrintUserEvent(ts: Long, userId: Long) extends Event

  trait HasUserPrinter[T] {
    var lastPrinted: Option[T] = None
    def updateLastPrinted(last: Option[T]) = {
      lastPrinted = last
    }
  }

  it should "implement average response time" in {
    class Analysis extends HasUserPrinter[Double] {
      import replaydb.event.MessageEvent

      val initiations: ReplayMap[(Long,Long), Long] = new ReplayMapImpl(???) // TODO don't actually have an update function
      val userAverages: ReplayMap[Long, ReplayAvg] = new ReplayMapImpl(new ReplayAvg(new ReplayCounterImpl))

      bind {
        me: MessageEvent =>
          initiations.put((me.senderUserId, me.recipientUserId), me.ts, me.ts)
      }
      bind {
        me: MessageEvent =>
          initiations.get((me.recipientUserId, me.senderUserId), me.ts) match {
            case Some(startTime) =>
              userAverages.update(me.senderUserId, _.add(me.ts - startTime, me.ts), me.ts)
            case None =>
          }
      }
      bind {
        e: PrintUserEvent =>
          updateLastPrinted(userAverages.get(e.userId, e.ts) match {
            case Some(avgVal) =>
              val avg: Double = avgVal.get(e.ts).get
              println(s"${e.userId} has average response time $avg")
              Some(avg)
            case None =>
              println(s"${e.userId} has no responses")
              None
          })
      }

      def update(x: Event) = emit(x)
    }

    val a = new Analysis
    a.update(new MessageEvent(ts = 10L, messageId = 1L, senderUserId =  100L, recipientUserId = 200L, content = ""))
    a.update(new MessageEvent(ts = 20L, messageId = 2L, senderUserId =  100L, recipientUserId = 300L, content = ""))
    a.update(new MessageEvent(ts = 30L, messageId = 3L, senderUserId =  200L, recipientUserId = 300L, content = ""))
    assert(a.lastPrinted === None)
    a.update(new PrintUserEvent(ts = 35L, userId = 200L))
    assert(a.lastPrinted === None)
    a.update(new MessageEvent(ts = 40L, messageId = 4L, senderUserId =  200L, recipientUserId = 100L, content = ""))
    a.update(new PrintUserEvent(ts = 50, userId = 200L))
    assert(a.lastPrinted === Some(30))
  }

  it should "implement fraction sent in response" in {
    class Analysis extends HasUserPrinter[Double] {
      val initiations: ReplayMap[(Long, Long), Int] = new ReplayMapImpl(???)
      val userAverages: ReplayMap[Long, ReplayAvg] = new ReplayMapImpl(new ReplayAvg(new ReplayCounterImpl))

      bind {
        me: MessageEvent =>
          initiations.put((me.senderUserId, me.recipientUserId), 1, me.ts)
      }

      bind {
        me: MessageEvent =>
          userAverages.update(me.senderUserId,
            _.add(
              initiations.get((me.recipientUserId, me.senderUserId), me.ts) match {
                case Some(_) => 1
                case None => 0
              }, me.ts
            ), me.ts
          )
      }

      bind {
        e: PrintUserEvent =>
          updateLastPrinted(userAverages.get(e.userId, e.ts) match {
            case Some(fracResponseVal) =>
              val frac: Double = fracResponseVal.get(e.ts).get
              println(s"${e.userId} has fraction sent in response $frac")
              Some(frac)
            case None =>
              println(s"${e.userId} has no responses")
              None
          })
      }

      def update(x: Event) = emit(x)
    }

    val a = new Analysis
    a.update(new MessageEvent(ts = 10L, messageId = 1L, senderUserId =  100L, recipientUserId = 200L, content = ""))
    a.update(new MessageEvent(ts = 20L, messageId = 2L, senderUserId =  100L, recipientUserId = 300L, content = ""))
    a.update(new MessageEvent(ts = 30L, messageId = 3L, senderUserId =  200L, recipientUserId = 300L, content = ""))
    assert(a.lastPrinted === None)
    a.update(new PrintUserEvent(ts = 35L, userId = 200L))
    assert(a.lastPrinted === Some(0D))
    a.update(new MessageEvent(ts = 40L, messageId = 4L, senderUserId =  200L, recipientUserId = 100L, content = ""))
    a.update(new PrintUserEvent(ts = 50, userId = 200L))
    assert(a.lastPrinted === Some(0.5D))
    a.update(new PrintUserEvent(ts = 51, userId = 100L))
    assert(a.lastPrinted === Some(0D))
    a.update(new PrintUserEvent(ts = 51, userId = 300L))
    assert(a.lastPrinted === None)
  }
}
