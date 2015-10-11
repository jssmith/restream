package replaydb.exec

import java.io.{FileInputStream, BufferedInputStream}

import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

object EventPrinter extends App {
  if (args.length > 1) {
    println("Usage: EventPrinter [ limit ]")
    System.exit(1)
  }
  val limit = if (args.length == 1) {
    Integer.parseInt(args(0))
  } else {
    100
  }
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => {
    println(e)
    pm.increment()
  }, limit)
  pm.finished()
}
