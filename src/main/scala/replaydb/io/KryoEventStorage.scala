package replaydb.io

import java.io.{BufferedInputStream, BufferedOutputStream, OutputStream, InputStream}

import com.esotericsoftware.kryo.KryoException
import replaydb.event.Event
import com.twitter.chill.ScalaKryoInstantiator
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * Event storage using the Kryo serialization protocol. Note that
 * this implementation requires registration, e.g. as,
 * {{{
 *   new KryoEventStorage {
 *     kryo.register(classOf[MessageEvent])
 *     kryo.register(classOf[NewFriendshipEvent])
 *     kryo.register(classOf[Array[String]])
 *   }
 * }}}
 *
 */
trait KryoEventStorage  {
  private val instantiator = new ScalaKryoInstantiator
  protected val kryo = instantiator.newKryo()

  // demanding performance so only serialize registered types
  kryo.setRegistrationRequired(true)

  def readEvents(is: InputStream, f: Event => Unit): Unit = {
    val input = new Input(new BufferedInputStream(is))
    var done = false
    while (!done) {
      try {
        f(kryo.readClassAndObject(input).asInstanceOf[Event])
      } catch {
        case e: KryoException =>
          input.close()
          done = true
      }
    }
  }

  def getEventWriter(os: OutputStream) = new EventWriter {
    val output = new Output(new BufferedOutputStream(os))
    override def write(e: Event): Unit = {
      kryo.writeClassAndObject(output, e)
    }
    override def close(): Unit = {
      output.close()
    }
  }
}

