package replaydb.exec

import java.io.{FileInputStream, BufferedInputStream}

import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

object EventPrinter extends App {
  if (args.length > 2) {
    println("Usage: EventPrinter filename [ limit ]")
    System.exit(1)
  }
  val fn = args(0)
  val limit = if (args.length == 2) {
    Integer.parseInt(args(1))
  } else {
    100
  }
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream(fn)), e => {
    println(e)
    pm.increment()
  }, limit)
  pm.finished()
}
