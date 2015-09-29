package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.io.KryoEventStorage
import replaydb.util.ProgressMeter

object EventCounter extends App {
  val eventStorage = new KryoEventStorage {
    kryo.register(classOf[MessageEvent])
    kryo.register(classOf[NewFriendshipEvent])
  }
  val pm = new ProgressMeter(1000000)
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => pm.increment())
  pm.finished()
}
