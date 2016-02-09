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
import scala.util.Random

// TODO ETK - I *think* this is cleaning up all unnecessary state (i.e. GC'ing itself)
//            but want to think more about it and make sure that's true

class StateCommunicationService(workerId: Int, numLocalPartitions: Int, runConfiguration: RunConfiguration) {

  val MaxMessagesPerCommand = 50000

  var readRequestsSatisfiedLocally = 0
  var readRequestsSatisfiedRemotely = 0
  var writesSentLocally = 0
  var writesSentRemotely = 0

  val logger = Logger(LoggerFactory.getLogger(classOf[StateCommunicationService]))
  val numPhases = runConfiguration.numPhases
  val numPartitions = runConfiguration.numPartitions
  val numWorkers = runConfiguration.hosts.length
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
  client.connect(runConfiguration.hosts.zipWithIndex.filter(_._2 != workerId).map(_._1))

  def close(): Unit = {
    client.closeWhenDone(true)
  }

  def issueCommandToWorker(wId: Int, cmd: Command): Unit = {
    // subtract 1 since the local partition isn't in the array
    client.issueCommand(if (wId > workerId) wId - 1 else wId, cmd)
  }

  // State is owned by worker #: partitionFn(key) % workerCount
  val states: mutable.Map[Int, ReplayMapImpl[Any, Any]] = mutable.HashMap()

  // stores read requests that have been received but can't yet be processed because not all writes have arrived
  val outstandingInboundReads: Array[Array[mutable.Map[Long, ArrayBuffer[StateUpdateCommand]]]] = Array.ofDim(numPhases, numWorkers)
  // stores writes that are going out to their respective partitions
  val queuedWrites: Array[Array[mutable.Map[Long, ArrayBuffer[StateWrite[_]]]]] = Array.ofDim(numPhases, numWorkers)
  // stores read prepares that are going out to their respective paritions
  val queuedReadPrepares: Array[Array[mutable.Map[Long, ArrayBuffer[StateRead]]]] = Array.ofDim(numPhases, numWorkers)

  val commandsSent: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)
  val partitionsFinished: Array[mutable.Map[Long, Int]] = Array.ofDim(numPhases)

  for (i <- 0 until numPhases; j <- 0 until numWorkers) {
    outstandingInboundReads(i)(j) = mutable.Map()
    commandsSent(i)(j) = mutable.Map()
    queuedWrites(i)(j) = mutable.Map()
    queuedReadPrepares(i)(j) = mutable.Map()
    if (j == 0) {
      partitionsFinished(i) = mutable.Map()
    }
  }

  // Send a request to the machine which owns this key
  def localPrepareState[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                              ts: Long, key: K, partitionFn: K => List[Int]): Unit = {
    val srcWorkers = partitionFn(key).map((i: Int) => (i & 0x7FFFFFFF) % numWorkers)
    // If it's available locally, grab from local, else grab from a random partition
    val srcWorker = srcWorkers.find(_ == workerId) match {
      case Some(id) => id
      case None => srcWorkers(Random.nextInt(srcWorkers.size))
    }
    this.synchronized {
      if (srcWorker == workerId) {
        readRequestsSatisfiedLocally += 1
      } else {
        readRequestsSatisfiedRemotely += 1
      }
      val queue = queuedReadPrepares(phaseId)(srcWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
      queue += StateRead(collectionId, ts, key)
      if (queue.size >= MaxMessagesPerCommand) {
        sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, -1, Array(), queue.toArray), srcWorker)
        queue.clear()
        commandsSent(phaseId)(srcWorker).put(batchEndTs, commandsSent(phaseId)(srcWorker).getOrElse(batchEndTs, 0) + 1)
      }
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                        ts: Long, key: K, merge: V => V, partitionFn: K => List[Int]): Unit = {
    val destWorkers = partitionFn(key).map((i: Int) => (i & 0x7FFFFFFF) % numWorkers).distinct
    this.synchronized {
      for (destWorker <- destWorkers) {
        if (destWorker == workerId) {
          writesSentLocally += 1
        } else {
          writesSentRemotely += 1
        }
        val queue = queuedWrites(phaseId)(destWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
        queue += StateWrite(collectionId, ts, key, merge)
        if (queue.size >= MaxMessagesPerCommand) {
          sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, -1, queue.toArray, Array()), destWorker)
          queue.clear()
          commandsSent(phaseId)(destWorker).put(batchEndTs, commandsSent(phaseId)(destWorker).getOrElse(batchEndTs, 0) + 1)
        }
      }
    }
  }

  def handleStateRequestResponse(resp: StateRequestResponse): Unit = {
    for (r <- resp.responses) {
      // TODO the typing here needs work...
      states(r.collectionId).insertPreparedValue(r.ts, r.key, r.value)
    }
  }

  def handleStateUpdateCommand(cmd: StateUpdateCommand): Unit = {
    logger.info(s"Phase $workerId-${cmd.phaseId} received writes from ${cmd.originatingWorkerId} for batch ${cmd.batchEndTs}")
    for (w <- cmd.writes) {
      // TODO the typing here needs work...
      states(w.collectionId).insertRemoteWrite(w.ts, w.key, w.asInstanceOf[StateWrite[Any]].merge)
    }
    val readRequestsToProcess = outstandingInboundReads.synchronized {
      val buf = outstandingInboundReads(cmd.phaseId)(cmd.originatingWorkerId).getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())
      buf += cmd
      if (buf.exists(_.cmdsInBatch == buf.size)
        && outstandingInboundReads(cmd.phaseId).map(_.getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())).forall(b => b.exists(_.cmdsInBatch == b.size))) {
        Some(outstandingInboundReads(cmd.phaseId).map(_.remove(cmd.batchEndTs).get).reduce(_ ++ _))
      } else {
        None
      }
    }
    if (readRequestsToProcess.isDefined) {
      logger.info(s"Phase $workerId-${cmd.phaseId} processing read requests for ${readRequestsToProcess.get.size} partitions")
      processStateReadRequests(readRequestsToProcess.get)
      // at this point, no more reads will be coming for this specific phase/batch...
      // Can GC after the second to last phase since the last phase doesn't read or write anything
      // (last phase's reads are carried out by the second to last phase)
      if (cmd.phaseId == numPhases - 2) {
        PerfLogger.logGc("Custom GC stats: (total merged values in collection, total unmerged values, number of ReplayValues, GC'd values)"
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
        if (cmd.originatingWorkerId == workerId) {
          handleStateRequestResponse(respCmd)
        } else {
          issueCommandToWorker(cmd.originatingWorkerId, respCmd)
        }
        rIdx += MaxMessagesPerCommand
      } while (rIdx < responses.length)
    }
  }

  // Closes out a batch: sends out all of the outstanding writes and getPrepares
  def finalizeBatch(phaseId: Int, batchEndTs: Long): Unit = {
    logger.info(s"Phase $workerId-$phaseId finalizing batch $batchEndTs")
    if (phaseId == numPhases - 1) {
      return // nothing to be done - final phase can't have any readPrepares or writes
    }
    this.synchronized {
      val numFinished = partitionsFinished(phaseId).getOrElse(batchEndTs, 0) + 1
      if (numFinished < numLocalPartitions) {
        partitionsFinished(phaseId)(batchEndTs) = numFinished
        return
      }
      for (i <- 0 until numWorkers) {
        val writes = queuedWrites(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
        val readPrepares = queuedReadPrepares(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer())
        val cmdsInBatch = 1 + (commandsSent(phaseId)(i).remove(batchEndTs) match {
          case Some(cnt) => cnt
          case _ => 0
        })
        sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, cmdsInBatch, writes.toArray, readPrepares.toArray), i)
        queuedWrites(phaseId)(i).remove(batchEndTs)
        queuedReadPrepares(phaseId)(i).remove(batchEndTs)
        commandsSent(phaseId)(i).remove(batchEndTs)
        partitionsFinished(phaseId).remove(batchEndTs)
      }
    }
  }

  def sendStateUpdateCommand(cmd: StateUpdateCommand, destPartition: Int): Unit = {
    if (destPartition != workerId) {
      logger.info(s"Phase $workerId-${cmd.phaseId} sending update to partition $destPartition for " +
        s"batch ${cmd.batchEndTs} (${cmd.writes.length} writes and ${cmd.readPrepares.length} readPrepares)")
      issueCommandToWorker(destPartition, cmd)
    } else {
      handleStateUpdateCommand(cmd)
    }
  }

  def registerReplayState(collectionId: Int, rs: ReplayMapImpl[_, _]): Unit = {
    if (states.contains(collectionId)) {
      throw new IllegalStateException("Can't have two collections with the same ID")
    }
    states += collectionId -> rs.asInstanceOf[ReplayMapImpl[Any, Any]]
  }

  def getReadWriteTrafficString: String = {
    s"Local Reads: $readRequestsSatisfiedLocally / Remote Reads $readRequestsSatisfiedRemotely // " +
      s"Local Writes $writesSentLocally / Remote Writes $writesSentRemotely"
  }

}

object StateCommunicationService {
  case class StateWrite[T](collectionId: Int, ts: Long, key: Any, merge: T => T)
  case class StateRead(collectionId: Int, ts: Long, key: Any)
  case class StateResponse(collectionId: Int, ts: Long, key: Any, value: Option[Any])
}
