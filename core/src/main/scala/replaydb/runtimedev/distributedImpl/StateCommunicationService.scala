package replaydb.runtimedev.distributedImpl

import com.typesafe.scalalogging.Logger
import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import replaydb.service.driver.{RunConfiguration, Command}
import replaydb.service.ClientGroupBase
import replaydb.runtimedev.distributedImpl.StateCommunicationService.{StateResponse, StateRead, StateWrite}
import replaydb.util.PerfLogger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO ETK - I *think* this is cleaning up all unnecessary state (i.e. GC'ing itself)
//            but want to think more about it and make sure that's true

class StateCommunicationService(partitionId: Int, runConfiguration: RunConfiguration) {
  
  val MaxMessagesPerCommand = 50000
  
  val logger = Logger(LoggerFactory.getLogger(classOf[StateCommunicationService]))
  val numPhases = runConfiguration.numPhases
  val numPartitions = runConfiguration.numPartitions
  val client = new ClientGroupBase(runConfiguration) {
    override def getHandler(): SimpleChannelUpstreamHandler = new SimpleChannelUpstreamHandler {
      override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
        me.getMessage.asInstanceOf[Command] match {
          case s: StateRequestResponse => {
            handleStateRequestResponse(s)
          }
        }
      }
      override def exceptionCaught(ctx: ChannelHandlerContext, ee: ExceptionEvent): Unit = {
        logger.error(s"caught exception ${ee.getCause} on local: ${ctx.getChannel.getLocalAddress} remote: ${ctx.getChannel.getRemoteAddress}")
      }
    }
  }
  client.connect(runConfiguration.hosts.zipWithIndex.filter(_._2 != partitionId).map(_._1))

  def close(): Unit = {
    client.closeWhenDone(true)
  }

  def issueCommandToWorker(wId: Int, cmd: Command): Unit = {
    // subtract 1 since the local partition isn't in the array
    client.issueCommand(if (wId > partitionId) wId - 1 else wId, cmd)
  }

  // State is owned by worker #: partitionFn(key) % workerCount
  val states: mutable.Map[Int, ReplayMapImpl[Any, Any]] = mutable.HashMap()

  // stores read requests that have been received but can't yet be processed because not all writes have arrived
  val outstandingInboundReads: Array[Array[mutable.Map[Long, ArrayBuffer[StateUpdateCommand]]]] = Array.ofDim(numPhases, numPartitions)
  // stores writes that are going out to their respective partitions
  val queuedWrites: Array[Array[mutable.Map[Long, ArrayBuffer[StateWrite[_]]]]] = Array.ofDim(numPhases, numPartitions)
  // stores read prepares that are going out to their respective paritions
  val queuedReadPrepares: Array[Array[mutable.Map[Long, ArrayBuffer[StateRead]]]] = Array.ofDim(numPhases, numPartitions)

  val commandsSent: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numPartitions)
  for (i <- 0 until numPhases; j <- 0 until numPartitions) {
    outstandingInboundReads(i)(j) = mutable.Map()
    commandsSent(i)(j) = mutable.Map()
    queuedWrites(i)(j) = mutable.Map()
    queuedReadPrepares(i)(j) = mutable.Map()
  }

  // Send a request to the machine which owns this key
  def localPrepareState[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                              ts: Long, key: K, partitionFn: K => Int): Unit = {
    val srcWorker = (partitionFn(key) & 0x7FFFFFFF) % numPartitions
    val queue = queuedReadPrepares(phaseId)(srcWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
    queue += StateRead(collectionId, ts, key)
    if (queue.size >= MaxMessagesPerCommand) {
      sendStateUpdateCommand(StateUpdateCommand(partitionId, phaseId, batchEndTs, false, Array(), queue.toArray), srcWorker)
      queue.clear()
      commandsSent(phaseId)(srcWorker).put(batchEndTs, commandsSent(phaseId)(srcWorker).getOrElse(batchEndTs, 0) + 1)
    }
  }

  def sendStateUpdateCommand(cmd: StateUpdateCommand, destPartition: Int): Unit = {
    if (destPartition != partitionId) {
      logger.info(s"Phase $partitionId-${cmd.phaseId} sending update to partition $destPartition for " +
        s"batch ${cmd.batchEndTs} (${cmd.writes.length} writes and ${cmd.readPrepares.length} readPrepares)")
      issueCommandToWorker(destPartition, cmd)
    } else {
      handleStateUpdateCommand(cmd)
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                        ts: Long, key: K, merge: V => V, partitionFn: K => Int): Unit = {
    val destWorker = (partitionFn(key) & 0x7FFFFFFF) % numPartitions
    val queue = queuedWrites(phaseId)(destWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
    queue += StateWrite(collectionId, ts, key, merge)
    if (queue.size >= MaxMessagesPerCommand) {
      sendStateUpdateCommand(StateUpdateCommand(partitionId, phaseId, batchEndTs, false, queue.toArray, Array()), destWorker)
      queue.clear()
      commandsSent(phaseId)(destWorker).put(batchEndTs, commandsSent(phaseId)(destWorker).getOrElse(batchEndTs, 0) + 1)
    }
  }

  def handleStateRequestResponse(resp: StateRequestResponse): Unit = {
    for (r <- resp.responses) {
      // TODO the typing here needs work...
      states(r.collectionId).insertPreparedValue(r.ts, r.key, r.value)
    }
  }

  def handleStateUpdateCommand(cmd: StateUpdateCommand): Unit = {
    logger.info(s"Phase $partitionId-${cmd.phaseId} received writes from ${cmd.originatingPartitionId} for batch ${cmd.batchEndTs}")
    val readRequestsToProcess = outstandingInboundReads.synchronized {
      val buf = outstandingInboundReads(cmd.phaseId)(cmd.originatingPartitionId).getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())
      buf += cmd
      if (cmd.finalized
        && outstandingInboundReads(cmd.phaseId).map(_.getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())).forall(b => b.nonEmpty && b.last.finalized)) {
        Some(outstandingInboundReads(cmd.phaseId).map(_.remove(cmd.batchEndTs).get).reduce(_ ++ _))
      } else {
        None
      }
    }
    for (w <- cmd.writes) {
      // TODO the typing here needs work...
      states(w.collectionId).insertRemoteWrite(w.ts, w.key, w.asInstanceOf[StateWrite[Any]].merge)
    }
    if (readRequestsToProcess.isDefined) {
      logger.info(s"Phase $partitionId-${cmd.phaseId} processing read requests for ${readRequestsToProcess.get.size} partitions")
      processStateReadRequests(readRequestsToProcess.get)
      // at this point, no more reads will be coming for this specific phase/batch...
      // Can GC after the second to last phase since the last phase doesn't read or write anything
      // (last phase's reads are carried out by the second to last phase)
      if (cmd.phaseId == numPhases - 2) {
        PerfLogger.log("Custom GC stats: (total merged values in collection, total unmerged values, number of ReplayValues, GC'd values)"
          + (for ((id, s) <- states) yield {
          s"State ID $id: ${s.gcOlderThan(cmd.batchEndTs)}"
        }).mkString(", "))
      }
    }
  }

  def processStateReadRequests(cmds: ArrayBuffer[StateUpdateCommand]): Unit = {
    for (cmd <- cmds) {
      val responses: ArrayBuffer[StateResponse] = ArrayBuffer()
      for (rp <- cmd.readPrepares) {
        val value = states(rp.collectionId).requestRemoteRead(rp.ts, rp.key)
        responses += StateResponse(rp.collectionId, rp.ts, rp.key, value)
      }
      var rIdx = 0
      do {
        val respCmd = StateRequestResponse(cmd.phaseId, cmd.batchEndTs,
          responses.slice(rIdx, rIdx + MaxMessagesPerCommand).toArray)
        if (cmd.originatingPartitionId == partitionId) {
          handleStateRequestResponse(respCmd)
        } else {
          issueCommandToWorker(cmd.originatingPartitionId, respCmd)
        }
        rIdx += MaxMessagesPerCommand
      } while (rIdx < responses.length)
    }
  }

  // Closes out a batch: sends out all of the outstanding writes and getPrepares
  def finalizeBatch(phaseId: Int, batchEndTs: Long): Unit = {
    logger.info(s"Phase $partitionId-$phaseId finalizing batch $batchEndTs")
    if (phaseId == numPhases - 1) {
      return // nothing to be done - final phase can't have any readPrepares or writes
    }
    for (i <- 0 until numPartitions) {
      val writes = queuedWrites(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
      val readPrepares = queuedReadPrepares(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
      sendStateUpdateCommand(StateUpdateCommand(partitionId, phaseId, batchEndTs, true, writes.toArray, readPrepares.toArray), i)
      queuedWrites(phaseId)(i).remove(batchEndTs)
      queuedReadPrepares(phaseId)(i).remove(batchEndTs)
      commandsSent(phaseId)(i).remove(batchEndTs)
    }
  }

  def registerReplayState(collectionId: Int, rs: ReplayMapImpl[_, _]): Unit = {
    if (states.contains(collectionId)) {
      throw new IllegalStateException("Can't have two collections with the same ID")
    }
    states += collectionId -> rs.asInstanceOf[ReplayMapImpl[Any, Any]]
  }

}

object StateCommunicationService {
  case class StateWrite[T](collectionId: Int, ts: Long, key: Any, merge: T => T)
  case class StateRead(collectionId: Int, ts: Long, key: Any)
  case class StateResponse(collectionId: Int, ts: Long, key: Any, value: Option[Any])
}
