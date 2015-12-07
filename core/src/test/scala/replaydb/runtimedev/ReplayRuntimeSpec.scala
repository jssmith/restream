package replaydb.runtimedev

import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import replaydb.event.{Event, MessageEvent, ProductUpdate, ProductView}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev.serialImpl._

class ReplayRuntimeSpec extends FlatSpec {
  val logger = Logger(LoggerFactory.getLogger(classOf[ReplayRuntimeSpec]))
  "A ReplayRuntime" should "implement overall popularity" in {
    case class StopEvent(ts: Long) extends Event
    var endCt = 0L
    class Analysis {
      val c: ReplayCounter = new ReplayCounterImpl
      val cSku: ReplayMap[Long,Long] = new ReplayMapImpl[Long, Long](0L)
      val allSkus: ReplayMap[Long,Int] = new ReplayMapImpl[Long,Int](0)

      def getRuntimeInterface = emit {
        bind { pu: ProductUpdate =>
          allSkus.merge(ts = pu.ts, key = pu.sku, fn = _ => 1)
        }
        bind { pv: ProductView =>
          c.add(1, pv.ts)
          cSku.merge(ts = pv.ts, key = pv.sku, fn = _ + 1)
        }
        bind { pv: ProductView =>
          val ct = c.get(pv.ts)
          logger.info("%f %b\n".format(
            if (ct > 0) {
              0D
            } else {
              cSku.get(pv.sku, pv.ts).getOrElse(0L).toDouble / ct.toDouble
            }, true))
            // TODO restore this test
//            printf("%f %b\n",
//              if (ct > 0) {
//                0D
//              } else {
//                cSku.get(allSkus.getRandom(pv.ts).orNull._1, pv.ts).getOrElse(0L).toDouble / ct.toDouble
//              }, false)
        }
        bind { se: StopEvent => endCt = c.get(se.ts)}
      }
    }

    val a = new Analysis
    val ai = a.getRuntimeInterface
    updateWithEvent(ai, new ProductUpdate(ts = 1L, sku = 100L))
    updateWithEvent(ai, new ProductView(ts = 2L, sku = 100L))
    updateWithEvent(ai, new ProductView(ts = 3L, sku = 100L))
    updateWithEvent(ai, new StopEvent(ts = 4L))
    assert(endCt === 2)
  }

  def updateWithEvent(ri: RuntimeInterface, e: Event): Unit = {
    ri.updateAllPhases(e)
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

      val initiations: ReplayMap[(Long,Long), Long] = new ReplayMapImpl[(Long,Long),Long](Long.MaxValue)
      val userAverages: ReplayMap[Long, ReplayAvg] = new ReplayMapImpl[Long,ReplayAvg](ReplayAvg.default)

      def getRuntimeInterface() = emit {
        bind {
          me: MessageEvent =>
            initiations.merge(ts = me.ts, key = (me.senderUserId, me.recipientUserId), prevMinTs => Math.min(prevMinTs, me.ts))
        }
        bind {
          me: MessageEvent =>
            initiations.get(ts = me.ts, key = (me.recipientUserId, me.senderUserId)) match {
              case Some(startTime) =>
                userAverages.merge(ts = me.ts, key = me.senderUserId, fn = ReplayAvg.add(me.ts - startTime))
              case None =>
            }
        }
        bind {
          e: PrintUserEvent =>
            updateLastPrinted(userAverages.get(ts = e.ts, key = e.userId) match {
              case Some(avgVal) =>
                avgVal.getAvg()
              case None =>
                None
            })
        }
      }
    }

    val a = new Analysis
    val ai = a.getRuntimeInterface()
    updateWithEvent(ai, new MessageEvent(ts = 10L, messageId = 1L, senderUserId =  100L, recipientUserId = 200L, content = ""))
    updateWithEvent(ai, new MessageEvent(ts = 20L, messageId = 2L, senderUserId =  100L, recipientUserId = 300L, content = ""))
    updateWithEvent(ai, new MessageEvent(ts = 30L, messageId = 3L, senderUserId =  200L, recipientUserId = 300L, content = ""))
    assert(a.lastPrinted === None)
    updateWithEvent(ai, new PrintUserEvent(ts = 35L, userId = 200L))
    assert(a.lastPrinted === None)
    updateWithEvent(ai, new MessageEvent(ts = 40L, messageId = 4L, senderUserId =  200L, recipientUserId = 100L, content = ""))
    updateWithEvent(ai, new PrintUserEvent(ts = 50, userId = 200L))
    assert(a.lastPrinted === Some(30))
  }

  it should "implement fraction sent in response" in {
    class Analysis extends HasUserPrinter[Double] {
      val initiations: ReplayMap[(Long, Long), Int] = new ReplayMapImpl(0)
      val userAverages: ReplayMap[Long, ReplayAvg] = new ReplayMapImpl(ReplayAvg.default)

      def getRuntimeInterface() = emit {
        bind {
          me: MessageEvent =>
            initiations.merge(ts = me.ts, key = (me.senderUserId, me.recipientUserId), fn = _ => 1)
        }

        bind {
          me: MessageEvent =>
            userAverages.merge(ts = me.ts, key = me.senderUserId,
              fn = ReplayAvg.add(initiations.get(ts = me.ts, key = (me.recipientUserId, me.senderUserId)) match {
                case Some(_) => 1
                case None => 0
              }))
        }

        bind {
          e: PrintUserEvent =>
            updateLastPrinted(userAverages.get(ts = e.ts, key = e.userId) match {
              case Some(fracResponseVal) =>
                fracResponseVal.getAvg()
              case None =>
                None
            })
        }
      }
    }

    val a = new Analysis
    val ai = a.getRuntimeInterface()
    updateWithEvent(ai, new MessageEvent(ts = 10L, messageId = 1L, senderUserId =  100L, recipientUserId = 200L, content = ""))
    updateWithEvent(ai, new MessageEvent(ts = 20L, messageId = 2L, senderUserId =  100L, recipientUserId = 300L, content = ""))
    updateWithEvent(ai, new MessageEvent(ts = 30L, messageId = 3L, senderUserId =  200L, recipientUserId = 300L, content = ""))
    assert(a.lastPrinted === None)
    updateWithEvent(ai, new PrintUserEvent(ts = 35L, userId = 200L))
    assert(a.lastPrinted === Some(0D))
    updateWithEvent(ai, new MessageEvent(ts = 40L, messageId = 4L, senderUserId =  200L, recipientUserId = 100L, content = ""))
    updateWithEvent(ai, new PrintUserEvent(ts = 50, userId = 200L))
    assert(a.lastPrinted === Some(0.5D))
    updateWithEvent(ai, new PrintUserEvent(ts = 51, userId = 100L))
    assert(a.lastPrinted === Some(0D))
    updateWithEvent(ai, new PrintUserEvent(ts = 51, userId = 300L))
    assert(a.lastPrinted === None)
  }

  it should "implement overall popularity with different bind order" in {
    case class StopEvent(ts: Long) extends Event
    var endCt = 0L
    class Analysis {
      val c: ReplayCounter = new ReplayCounterImpl
      val cSku: ReplayMap[Long,Long] = new ReplayMapImpl[Long, Long](0L)
      val allSkus: ReplayMap[Long,Int] = new ReplayMapImpl[Long,Int](0)

      def getRuntimeInterface = emit {
        bind { pv: ProductView =>
          val ct = c.get(pv.ts)
          logger.info("%f %b\n".format(
            if (ct > 0) {
              0D
            } else {
              cSku.get(pv.sku, pv.ts).getOrElse(0L).toDouble / ct.toDouble
            }, true))
            // TODO restore this test
//            printf("%f %b\n",
//              if (ct > 0) {
//                0D
//              } else {
//                cSku.get(allSkus.getRandom(pv.ts).orNull._1, pv.ts).getOrElse(0L).toDouble / ct.toDouble
//              }, false)
        }
        bind { se: StopEvent => endCt = c.get(se.ts)}
        bind { pv: ProductView =>
          c.add(1, pv.ts)
          cSku.merge(ts = pv.ts, key = pv.sku, fn = _ + 1)
        }
        bind { pu: ProductUpdate =>
          allSkus.merge(ts = pu.ts, key = pu.sku, fn = _ => 1)
        }
      }
    }

    val a = new Analysis
    val ai = a.getRuntimeInterface
    updateWithEvent(ai, new ProductUpdate(ts = 1L, sku = 100L))
    updateWithEvent(ai, new ProductView(ts = 2L, sku = 100L))
    updateWithEvent(ai, new ProductView(ts = 3L, sku = 100L))
    updateWithEvent(ai, new StopEvent(ts = 4L))
    assert(endCt === 2)
  }
}
