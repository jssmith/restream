package replaydb.service.driver

import com.twitter.chill.ScalaKryoInstantiator

trait KryoCommands {
  private val instantiator = new ScalaKryoInstantiator
  protected val kryo = instantiator.newKryo()

  kryo.setRegistrationRequired(true)
  kryo.register(classOf[Array[String]])
  kryo.register(classOf[CloseCommand])
  kryo.register(classOf[InitReplayCommand[_]])
  kryo.register(classOf[ProgressUpdateCommand])
  kryo.register(classOf[UpdateAllProgressCommand])
}


object KryoCommands {
  val MAX_KRYO_MESSAGE_SIZE = 10000
}