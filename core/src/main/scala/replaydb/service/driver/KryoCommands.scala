package replaydb.service.driver

import com.twitter.chill.ScalaKryoInstantiator
import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateWrite, StateRead, StateResponse}
import replaydb.runtimedev.distributedImpl.{StateRequestResponse, StateUpdateCommand}

trait KryoCommands {
  private val instantiator = new ScalaKryoInstantiator
  protected val kryo = instantiator.newKryo()

//  kryo.setRegistrationRequired(true)
  kryo.register(classOf[Array[String]])
  kryo.register(classOf[CloseCommand])
  kryo.register(classOf[CloseWorkerCommand])
  kryo.register(classOf[InitReplayCommand[_]])
  kryo.register(classOf[ProgressUpdateCommand])
  kryo.register(classOf[UpdateAllProgressCommand])
  kryo.register(classOf[StateUpdateCommand])
  kryo.register(classOf[StateRequestResponse])
  kryo.register(classOf[StateResponse])
  kryo.register(classOf[StateRead])
  kryo.register(classOf[StateWrite[_]])
  kryo.register(classOf[Array[StateResponse]])
  kryo.register(classOf[Array[StateRead]])
  kryo.register(classOf[Array[StateWrite[_]]])
  kryo.register(classOf[RunConfiguration])
  kryo.register(classOf[Hosts.HostConfiguration])
  kryo.register(classOf[Array[Hosts.HostConfiguration]])
}


object KryoCommands {
  val MAX_KRYO_MESSAGE_SIZE = 50000000
}