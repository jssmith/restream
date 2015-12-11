package replaydb.service.driver

import com.twitter.chill.ScalaKryoInstantiator
import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateWrite, StateRead, StateResponse}
import replaydb.runtimedev.distributedImpl.{ReplayCounterImpl, StateRequestResponse, StateUpdateCommand}

import scala.collection.mutable.ArrayBuffer

trait KryoCommands {
  private val instantiator = new ScalaKryoInstantiator
  val kryo = instantiator.newKryo()

//  kryo.setRegistrationRequired(true)
//  kryo.register(ReplayCounterImpl.mergeFunction(0).getClass)
  kryo.register(classOf[Array[_]])
  kryo.register(classOf[ArrayBuffer[_]])
  kryo.register(None.getClass)
  kryo.register(classOf[Some[_]])
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

  def registerTypes(types: Array[Class[_]]): Unit = {
    types.foreach(kryo.register)
  }
}

object KryoCommands {
  val MAX_KRYO_MESSAGE_SIZE = 10000000
}