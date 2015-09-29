package replaydb.exec

import java.io.{BufferedOutputStream, FileOutputStream}

import replaydb.event._
import replaydb.io.KryoEventStorage
import replaydb.util.ProgressMeter
import replaydb.{EventSource, util}

import scala.util.Random

/**
 * Simple spam simulation event-generator functionality
 */
object EventGenerator extends App {
  if (args.length != 2) {
    println("Usage: MessageGenerator numUsers numEvents")
    System.exit(1)
  }
  val numUsers = Integer.parseInt(args(0))
  val numEvents = Integer.parseInt(args(1))
  var startTime = util.Date.df.parse("2015-01-01 00:00:00.000").getTime

  val rnd =  new Random(903485435L)
  val eventSource = new EventSource(startTime, numUsers, rnd)
  val eventStorage = new KryoEventStorage {
    kryo.register(classOf[MessageEvent])
    kryo.register(classOf[NewFriendshipEvent])
  }
  val pm = new ProgressMeter(1000000)
  val w = eventStorage.getEventWriter(new BufferedOutputStream(new FileOutputStream("/tmp/events.out")))
  try {
    eventSource.genEvents(numEvents, e => {
      w.write(e)
      pm.increment()
    })
  } finally {
    w.close()
  }
  pm.finished()
}
