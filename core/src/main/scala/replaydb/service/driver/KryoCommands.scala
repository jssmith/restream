package replaydb.service.driver

import com.twitter.chill.ScalaKryoInstantiator
import replaydb.runtimedev.distributedImpl.{StateRequestResponse, StateRequestCommand, StateWriteCommand}

trait KryoCommands {
  private val instantiator = new ScalaKryoInstantiator
  protected val kryo = instantiator.newKryo()

  kryo.setRegistrationRequired(true)
  kryo.register(classOf[Array[String]])
  kryo.register(classOf[CloseCommand])
  kryo.register(classOf[InitReplayCommand[_]])
  kryo.register(classOf[ProgressUpdateCommand])
  kryo.register(classOf[UpdateAllProgressCommand])
  kryo.register(classOf[StateWriteCommand[_]])
  kryo.register(classOf[StateRequestCommand])
  kryo.register(classOf[StateRequestResponse])
  kryo.register(classOf[RunConfiguration])
  kryo.register(classOf[Hosts.HostConfiguration])
  kryo.register(classOf[Array[Hosts.HostConfiguration]])
}


object KryoCommands {
  val MAX_KRYO_MESSAGE_SIZE = 10000
}