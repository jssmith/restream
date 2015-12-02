package replaydb.runtimedev.distributedImpl

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInboundHandler}
import replaydb.service.driver.{RunConfiguration, Command}
import replaydb.service.{ClientGroupBase}
import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateResponse, StateRead, StateWrite}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StateCommunicationService(workerId: Int, runConfiguration: RunConfiguration) {
  val workerCount = runConfiguration.hosts.length
  val client = new ClientGroupBase(runConfiguration) {
    override def getHandler(): ChannelInboundHandler = {
      new ChannelInboundHandlerAdapter() {
        override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
          msg.asInstanceOf[Command] match {
            case s: StateRequestResponse => {
              handleStateRequestResponse(s)
            }
          }
        }
      }
    }
  }
  client.connect(runConfiguration.hosts.zipWithIndex.filter(_._2 != workerId).map(_._1))

  def close(): Unit = {
    client.closeWhenDone(true)
  }

  // State is owned by worker #: partitionFn(key) % workerCount

  // array index: phaseId. entries: map of batchId -> outstandingRequests
  //    (number of read requests not yet received a response, must be 0 to advance)
//  val outstandingReadRequests: Array[mutable.Map[Int, Long]] = Array.ofDim(numPhases)
  // not using ^ for now, just going to let worker threads block on *get* if the
  // state isn't there which should achieve basically the same thing

  // array index: phaseId. entries: map of batchId -> expected writes
  //    (number of partitions from which we expect to receive writes but have not yet,
  //     cannot service a read request until this is 0)
//  val expectedWritesRemaining: Array[mutable.Map[Long, Int]] = Array.ofDim(numPhases)
  val outstandingInboundReads: Array[mutable.Map[Long, ArrayBuffer[StateUpdateCommand]]] = Array.ofDim(runConfiguration.numPhases)
  val queuedWrites: Array[Array[mutable.Map[Long, ArrayBuffer[StateWrite[_]]]]] = Array.ofDim(runConfiguration.numPhases, workerCount)
  val queuedReadPrepares: Array[Array[mutable.Map[Long, ArrayBuffer[StateRead]]]] = Array.ofDim(runConfiguration.numPhases, workerCount)
  for (i <- 0 until runConfiguration.numPhases) {
    //      outstandingReadRequests(i) = mutable.Map()
//    expectedWritesRemaining(i) = mutable.Map()
    outstandingInboundReads(i) = mutable.Map()
    for (j <- 0 until workerCount) {
      queuedWrites(i)(j) = mutable.Map()
      queuedReadPrepares(i)(j) = mutable.Map()
    }
  }

  // Send a request to the machine which owns this key
  def localPrepareState[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                              ts: Long, key: K, partitionFn: K => Int): Unit = {
    val srcWorker = partitionFn(key) % workerCount
    if (srcWorker == workerId) {
      val rs = states(collectionId).asInstanceOf[ReplayMapImpl[K, V]]
      rs.insertPreparedValue(ts, key, rs.get(ts, key))
    } else {
      queuedReadPrepares(phaseId)(srcWorker).getOrElseUpdate(batchEndTs, ArrayBuffer()) +=
        StateRead(collectionId, ts, key)
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                        ts: Long, key: K, merge: V => V, partitionFn: K => Int): Unit = {
    val destWorker = partitionFn(key) % workerCount
    if (destWorker == workerId) {
      states(collectionId).asInstanceOf[ReplayMapImpl[K, V]].insertRemoteWrite(ts, key, merge)
    } else {
      queuedWrites(phaseId)(destWorker).getOrElseUpdate(batchEndTs, ArrayBuffer()) +=
        StateWrite(collectionId, ts, key, merge)
    }
  }

  def handleStateRequestResponse(resp: StateRequestResponse): Unit = {
    for (r <- resp.responses) {
      // TODO the typing here needs work...
      states(r.collectionId).asInstanceOf[ReplayMapImpl[Any, Any]].insertPreparedValue(r.ts, r.key, r.value)
    }
  }

  def handleStateUpdateCommand(cmd: StateUpdateCommand): Unit = {
//    val readRequestsToProcess = expectedWritesRemaining.synchronized {
//      val currentExpectedWrites =
//        expectedWritesRemaining(cmd.phaseId).getOrElse(cmd.batchEndTs, runConfiguration.numPartitions - 1) - 1
//      if (currentExpectedWrites == 0) {
//        expectedWritesRemaining(cmd.phaseId).remove(cmd.batchEndTs)
//        true
//      } else {
//        expectedWritesRemaining(cmd.phaseId)(cmd.batchEndTs) = currentExpectedWrites
//        false
//      }
//    }
    val readRequestsToProcess = outstandingInboundReads.synchronized {
      val buf = outstandingInboundReads(cmd.phaseId).getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())
      buf += cmd
      if (buf.size == runConfiguration.numPartitions - 1) {
        outstandingInboundReads(cmd.phaseId).remove(cmd.batchEndTs)
      } else {
        None
      }
    }
    for (w <- cmd.writes) {
      // TODO the typing here needs work...
      states(w.collectionId).asInstanceOf[ReplayMapImpl[Any, Any]].insertRemoteWrite(w.ts, w.key, w.asInstanceOf[StateWrite[Any]].merge)
    }
    if (readRequestsToProcess.isDefined) {
      processStateReadRequests(readRequestsToProcess.get)
    }
  }

  def processStateReadRequests(cmds: ArrayBuffer[StateUpdateCommand]): Unit = {
    for (cmd <- cmds) {
      val responses: ArrayBuffer[StateResponse] = ArrayBuffer()
      for (rp <- cmd.readPrepares) {
        // TODO the typing here needs work...
        val value = states(rp.collectionId).asInstanceOf[ReplayMapImpl[Any, Any]].get(rp.ts, rp.key)
        responses += StateResponse(rp.collectionId, rp.ts, rp.key, value)
      }
      // TODO does this require a new thread? executor?
      val respCmd = StateRequestResponse(cmd.phaseId, cmd.batchEndTs, responses.toArray)
      client.issueCommand(cmd.originatingPartitionId, respCmd)
    }
  }

  // TODO something needs to call this
  // Closes out a batch: sends out all of the outstanding writes and getPrepares
  def finalizeBatch(phaseId: Int, batchEndTs: Long): Unit = {
    for (i <- 0 until workerCount if i != workerId) {
      val writes = queuedWrites(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
      val readPrepares = queuedReadPrepares(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
      val cmd = StateUpdateCommand(workerId, phaseId, batchEndTs, writes.toArray, readPrepares.toArray)
      // TODO does this require a new thread? executor?
      client.issueCommand(i, cmd)
      queuedWrites(phaseId)(i).remove(batchEndTs)
      queuedReadPrepares(phaseId)(i).remove(batchEndTs)
    }
  }

  val states: mutable.Map[Int, ReplayMapImpl[_, _]] = mutable.HashMap()

  def registerReplayState(collectionId: Int, rs: ReplayMapImpl[_, _]): Unit = {
    if (states.contains(collectionId)) {
      throw new IllegalStateException("Can't have two collections with the same ID")
    }
    states += collectionId -> rs
  }

}

object StateCommunicationService {
  case class StateWrite[T](collectionId: Int, ts: Long, key: Any, merge: T => T)
  case class StateRead(collectionId: Int, ts: Long, key: Any)
  case class StateResponse(collectionId: Int, ts: Long, key: Any, value: Option[Any])
}
