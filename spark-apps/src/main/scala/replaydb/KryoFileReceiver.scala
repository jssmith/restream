package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}

class KryoFileReceiver(filename: String)
    extends Receiver[Event](StorageLevel.MEMORY_AND_DISK_2) {

    def onStart() {
      // Start the thread that receives data over a connection
      new Thread("File Receiver") {
        override def run() { receive() }
      }.start()
    }

    def onStop() {
      // There is nothing much to do as the thread calling receive()
      // is designed to stop by itself if isStopped() returns false
    }

    /** Open a file for reading and receive data until receiver is stopped */
    private def receive() {
      val instantiator = new ScalaKryoInstantiator
      val kryo = instantiator.newKryo()
      kryo.register(classOf[MessageEvent])
      kryo.register(classOf[NewFriendshipEvent])

      val input = new Input(new BufferedInputStream(new FileInputStream(filename)))

      var eventsRead = 0
      while (!isStopped()) {
        try {
          store(kryo.readClassAndObject(input).asInstanceOf[Event])
          eventsRead += 1
        } catch {
          case e: KryoException =>
            println(s"KRYO IS CLOSING; $eventsRead EVENTS READ")
            Thread.sleep(10000)
            input.close()
//            stop("Reached end of file!")
        }
      }
    }

}
