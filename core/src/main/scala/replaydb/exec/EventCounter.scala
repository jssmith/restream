package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.exec.EventGenerator._
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

object EventCounter extends App {
  if (args.length != 1) {
    println(
      """Usage: EventCounter filename
      """.stripMargin)
    System.exit(1)
  }
  var filename = args(0)

  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream(filename)), e => pm.increment())
  pm.finished()
}
