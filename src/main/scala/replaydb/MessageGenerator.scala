package replaydb

import scala.util.Random

/**
 * Simple spam simulation event-generator functionality
 */
object MessageGenerator extends App {
  if (args.length != 2) {
    println("Usage: MessageGenerator numUsers numEvents")
    System.exit(1)
  }
  val numUsers = Integer.parseInt(args(0))
  val numEvents = Integer.parseInt(args(1))
  var startTime = util.Date.df.parse("2015-01-01 00:00:00.000").getTime

  val rnd =  new Random(903485435L)
  val es = new EventSource(startTime, numUsers, rnd)
  es.genEvents(numEvents, e => println(e))
}
