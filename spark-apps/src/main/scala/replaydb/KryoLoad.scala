package replaydb

import java.io.{BufferedInputStream, FileInputStream}

import com.esotericsoftware.kryo.KryoException
import com.twitter.chill.{Input, ScalaKryoInstantiator}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}


object KryoLoad {

  def loadFiles(sc: SparkContext, filenames: Seq[String]): RDD[Event] = {
    val partitions = filenames.length
    sc.parallelize(filenames, partitions).flatMap(KryoLoad.load)
  }

  def load(filename: String): Iterator[Event] = {
    val instantiator = new ScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.register(classOf[MessageEvent])
    kryo.register(classOf[NewFriendshipEvent])

    new Iterator[Event] {
      val input = new Input(new BufferedInputStream(new FileInputStream(filename)))
      var done = false
      var nextEvent: Event = null

      override def next: Event = {
        hasNext
        val retEvent = nextEvent
        nextEvent = null
        retEvent
      }

      override def hasNext: Boolean = {
        if (done) {
          return false
        }
        if (nextEvent == null) {
          try {
            nextEvent = kryo.readClassAndObject(input).asInstanceOf[Event]
          } catch {
            case e: KryoException =>
              input.close()
              done = true
          }
        }
        nextEvent != null
      }
    }
  }

}
