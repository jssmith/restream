package replaydb.runtimedev.distributedImpl

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInboundHandler}
import replaydb.service.driver.{RunConfiguration, Command}
import replaydb.service.{ClientGroupBase}

import scala.collection.mutable

class StateCommunicationService(workerId: Int, runConfiguration: RunConfiguration) {
  val workerCount = runConfiguration.hosts.length
  val client = new ClientGroupBase(runConfiguration) {
    override def getHandler(): ChannelInboundHandler = {
      new ChannelInboundHandlerAdapter() {
        override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
          msg.asInstanceOf[Command] match {
            case s: StateRequestResponse => {
              // TODO - have to get the types right here
//              val rs: ReplayMapImpl[_,_] = states(s.collectionId)
//              rs.insertPreparedValue(s.ts, s.key, Some(s.value))
            }
          }
        }
      }
    }
  }
  client.connect(runConfiguration.hosts.zipWithIndex.filter(_._2 != workerId).map(_._1))

  // State is owned by worker #: partitionFn(key) % workerCount

  // Send a request to the machine which owns this key
  def localPrepareState[K, V](collectionId: Int, ts: Long, key: K, partitionFn: K => Int): Unit = {
    val srcWorker = partitionFn(key) % workerCount
    if (srcWorker == workerId) {
      val rs = states(collectionId).asInstanceOf[ReplayMapImpl[K, V]]
      rs.insertPreparedValue(ts, key, rs.get(ts, key))
    } else {
      val cmd = new StateRequestCommand(collectionId, ts, key)
      client.issueCommand(srcWorker, cmd)
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, ts: Long, key: K, merge: V => V, partitionFn: K => Int): Unit = {
    val destWorker = partitionFn(key) % workerCount
    if (destWorker == workerId) {
      states(collectionId).asInstanceOf[ReplayMapImpl[K, V]].insertRemoteWrite(ts, key, merge)
    } else {
      val cmd = StateWriteCommand(collectionId, ts, key, merge)
      client.issueCommand(destWorker, cmd)
    }
  }

  def handleStateWriteCommand[T](cmd: StateWriteCommand[T]) = {
    // TODO - have to get the types right here
//    states(cmd.collectionId).insertRemoteWrite(cmd.ts, cmd.key, cmd.merge)
  }

  def handleStateRequestCommand(cmd: StateRequestCommand): StateRequestResponse = {
    // TODO - have to get the types right here
//    val rv = states(cmd.collectionId).get(cmd.ts, cmd.key)
//    new StateRequestResponse(cmd.collectionId, cmd.ts, cmd.key, rv)
    ???
  }

  val states: mutable.Map[Int, ReplayMapImpl[_, _]] = mutable.HashMap()

  def registerReplayState(collectionId: Int, rs: ReplayMapImpl[_, _]): Unit = {
    if (states.contains(collectionId)) {
      throw new IllegalStateException("Can't have two collections with the same ID")
    }
    states += collectionId -> rs
  }

}
