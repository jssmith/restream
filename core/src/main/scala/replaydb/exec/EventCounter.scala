package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

object EventCounter extends App {
  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => pm.increment())
  pm.finished()
}
