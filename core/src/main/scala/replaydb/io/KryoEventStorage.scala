package replaydb.io

import java.io._

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

  def readEvents(is: InputStream, f: Event => Unit, limit: Long = Long.MaxValue): Unit = {
    val input = new Input(new BufferedInputStream(is))
    var done = false
    var ct = 0
    while (!done && ct < limit) {
      try {
        f(kryo.readClassAndObject(input).asInstanceOf[Event])
        ct += 1
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

  def getSplitEventWriter(fnBase: String, n: Int) = new EventWriter {
    val outputs =
      ((0 until n) map (n => s"$fnBase-$n") map (
        fn => getEventWriter(new FileOutputStream(fn)))).toArray
    var index = 0

    override def write(e: Event): Unit = {
      outputs(index).write(e)
      index = (index + 1) % n
    }

    override def close(): Unit = {
      for (output <- outputs) {
        try {
          output.close()
        } catch {
          case _: IOException => System.err.println("problem closing stream")
        }
      }
    }
  }
}

