package replaydb.service.driver

import com.twitter.chill.ScalaKryoInstantiator
import replaydb.runtimedev.distributedImpl.ReplayAccumulatorImpl.{StateResponseAccum, StateReadAccum, StateWriteAccum}
import replaydb.runtimedev.distributedImpl.ReplayMapImpl.{StateReadMap, StateWriteMap, StateResponseMap}
import replaydb.runtimedev.distributedImpl.ReplayMapTopKImpl.{StateReadTopK, StateResponseTopK}
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
  kryo.register(classOf[StateWrite])
  kryo.register(classOf[Array[StateResponse]])
  kryo.register(classOf[Array[StateRead]])
  kryo.register(classOf[Array[StateWrite]])

  kryo.register(classOf[StateResponseMap[_,_]])
  kryo.register(classOf[StateReadMap[_]])
  kryo.register(classOf[StateWriteMap[_,_]])
  kryo.register(classOf[Array[StateResponseMap[_,_]]])
  kryo.register(classOf[Array[StateReadMap[_]]])
  kryo.register(classOf[Array[StateWriteMap[_,_]]])
  kryo.register(classOf[StateResponseAccum])
  kryo.register(classOf[StateReadAccum])
  kryo.register(classOf[StateWriteAccum])
  kryo.register(classOf[Array[StateResponseAccum]])
  kryo.register(classOf[Array[StateReadAccum]])
  kryo.register(classOf[Array[StateWriteAccum]])
  kryo.register(classOf[StateResponseTopK[_,_]])
  kryo.register(classOf[StateReadTopK])
  kryo.register(classOf[Array[StateResponseTopK[_,_]]])
  kryo.register(classOf[Array[StateReadTopK]])

  kryo.register(classOf[RunConfiguration])
  kryo.register(classOf[Hosts.HostConfiguration])
  kryo.register(classOf[Array[Hosts.HostConfiguration]])
}


object KryoCommands {
  val MAX_KRYO_MESSAGE_SIZE = 10000000
}