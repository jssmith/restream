package replaydb.runtimedev.distributedImpl

import java.util.concurrent.locks.{Lock, Condition, ReentrantLock}

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

class StateCommunicationService(workerId: Int, numLocalPartitions: Int, runConfiguration: RunConfiguration) {

  val MaxMessagesPerCommand = 50000

  var readRequestsSatisfiedLocally = 0
  var readRequestsSatisfiedRemotely = 0
  var writesSentLocally = 0
  var writesSentRemotely = 0

  val waitAtBatchBoundary = runConfiguration.waitAtBatchBoundary

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
  val queuedReadPrepareCount: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)

  val commandsSent: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)
  val partitionsFinished: Array[mutable.Map[Long, Int]] = Array.ofDim(numPhases)

  val generalLocks: Array[Array[Lock]] = Array.ofDim(numPhases, numWorkers)
  val queuedReadPrepareCountLocks: Array[Array[Lock]] = Array.ofDim(numPhases,numWorkers)
  val queuedReadPrepareCountConditions: Array[Array[Condition]] = Array.ofDim(numPhases, numWorkers)
  val outstandingInboundReadsLocks: Array[Lock] = Array.ofDim(numPhases)
  val partitionsFinishedLocks: Array[Lock] = Array.ofDim(numPhases)

  for (i <- 0 until numPhases; j <- 0 until numWorkers) {
    outstandingInboundReads(i)(j) = mutable.Map()
    commandsSent(i)(j) = mutable.Map()
    queuedWrites(i)(j) = mutable.Map()
    queuedReadPrepares(i)(j) = mutable.Map()
    queuedReadPrepareCount(i)(j) = mutable.Map()

    generalLocks(i)(j) = new ReentrantLock()
    queuedReadPrepareCountLocks(i)(j) = new ReentrantLock()
    queuedReadPrepareCountConditions(i)(j) = queuedReadPrepareCountLocks(i)(j).newCondition()
    if (j == 0) {
      outstandingInboundReadsLocks(i) = new ReentrantLock()
      partitionsFinishedLocks(i) = new ReentrantLock()
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
    if (srcWorker == workerId) {
      readRequestsSatisfiedLocally += 1
    } else {
      readRequestsSatisfiedRemotely += 1
    }
    generalLocks(phaseId)(srcWorker).lock()
    val queue = queuedReadPrepares(phaseId)(srcWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
    queue += StateRead(collectionId, ts, key)
    val messagesToSend = if (queue.size >= MaxMessagesPerCommand) {
      commandsSent(phaseId)(srcWorker).put(batchEndTs, commandsSent(phaseId)(srcWorker).getOrElse(batchEndTs, 0) + 1)
      val q = queue.toArray
      queue.clear()
      q
    } else {
      Array[StateRead]()
    }
    generalLocks(phaseId)(srcWorker).unlock()
    if (waitAtBatchBoundary) {
      queuedReadPrepareCountLocks(phaseId+1)(workerId).lock()

      queuedReadPrepareCount(phaseId+1)(workerId).put(batchEndTs,
        queuedReadPrepareCount(phaseId+1)(workerId).getOrElse(batchEndTs, 0) + 1)

      queuedReadPrepareCountLocks(phaseId+1)(workerId).unlock()
    }
    if (messagesToSend.nonEmpty) {
      sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, -1, Array(), messagesToSend), srcWorker)
    }
  }

  // send this write to its appropriate partition to be stored
  def submitWrite[K, V](collectionId: Int, phaseId: Int, batchEndTs: Long,
                        ts: Long, key: K, merge: V => V, partitionFn: K => List[Int]): Unit = {
    val destWorkers = partitionFn(key).map((i: Int) => (i & 0x7FFFFFFF) % numWorkers).distinct
    for (destWorker <- destWorkers) {
      if (destWorker == workerId) {
        writesSentLocally += 1
      } else {
        writesSentRemotely += 1
      }
      generalLocks(phaseId)(destWorker).lock()
      val queue = queuedWrites(phaseId)(destWorker).getOrElseUpdate(batchEndTs, ArrayBuffer())
      queue += StateWrite(collectionId, ts, key, merge)
      val messagesToSend = if (queue.size >= MaxMessagesPerCommand) {
        commandsSent(phaseId)(destWorker).put(batchEndTs, commandsSent(phaseId)(destWorker).getOrElse(batchEndTs, 0) + 1)
        val q = queue.toArray
        queue.clear()
        q
      } else {
        Array[StateWrite[_]]()
      }
      generalLocks(phaseId)(destWorker).lock()
      if (messagesToSend.nonEmpty) {
        sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, -1, messagesToSend, Array()), destWorker)
      }
    }
  }

  def handleStateRequestResponse(resp: StateRequestResponse): Unit = {
    if (waitAtBatchBoundary) {
      queuedReadPrepareCountLocks(resp.phaseId+1)(workerId).lock()
      val remainingCount =
        queuedReadPrepareCount(resp.phaseId+1)(workerId).getOrElse(resp.batchEndTs, 0) - resp.responses.length
      queuedReadPrepareCount(resp.phaseId+1)(workerId).put(resp.batchEndTs, remainingCount)
      if (remainingCount == 0) {
        queuedReadPrepareCountConditions(resp.phaseId+1)(workerId).signalAll()
      }
      queuedReadPrepareCountLocks(resp.phaseId+1)(workerId).unlock()
    }
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
    outstandingInboundReadsLocks(cmd.phaseId).lock()

    val buf = outstandingInboundReads(cmd.phaseId)(cmd.originatingWorkerId).getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())
    buf += cmd
    val readRequestsToProcess = if (buf.exists(_.cmdsInBatch == buf.size)
      && outstandingInboundReads(cmd.phaseId).map(_.getOrElseUpdate(cmd.batchEndTs, ArrayBuffer())).forall(b => b.exists(_.cmdsInBatch == b.size))) {
      Some(outstandingInboundReads(cmd.phaseId).map(_.remove(cmd.batchEndTs).get).reduce(_ ++ _))
    } else {
      None
    }

    outstandingInboundReadsLocks(cmd.phaseId).unlock()

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

  def awaitReadsReady(phaseId: Int, batchEndTs: Long): Unit = {
    if (waitAtBatchBoundary) {
      queuedReadPrepareCountLocks(phaseId)(workerId).lock()

      while (queuedReadPrepareCount(phaseId)(workerId).getOrElse(batchEndTs, 0) != 0) {
        queuedReadPrepareCountConditions(phaseId)(workerId).await()
      }
      queuedReadPrepareCount(phaseId)(workerId).remove(batchEndTs)

      queuedReadPrepareCountLocks(phaseId)(workerId).unlock()
    }
  }

  // Closes out a batch: sends out all of the outstanding writes and getPrepares
  def finalizeBatch(phaseId: Int, batchEndTs: Long): Unit = {
    logger.info(s"Phase $workerId-$phaseId finalizing batch $batchEndTs")
    if (phaseId == numPhases - 1) {
      return // nothing to be done - final phase can't have any readPrepares or writes
    }

    partitionsFinishedLocks(phaseId).lock()
    val numFinished = partitionsFinished(phaseId).getOrElse(batchEndTs, 0) + 1
    if (numFinished < numLocalPartitions) {
      partitionsFinished(phaseId)(batchEndTs) = numFinished
      partitionsFinishedLocks(phaseId).unlock()
      return
    }
    partitionsFinished(phaseId).remove(batchEndTs)
    partitionsFinishedLocks(phaseId).unlock()

    for (i <- 0 until numWorkers) {
      generalLocks(phaseId)(i).lock()
      val writes = queuedWrites(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer()).toArray
      val readPrepares = queuedReadPrepares(phaseId)(i).getOrElse(batchEndTs, ArrayBuffer()).toArray
      val cmdsInBatch = 1 + (commandsSent(phaseId)(i).remove(batchEndTs) match {
        case Some(cnt) => cnt
        case _ => 0
      })
      queuedWrites(phaseId)(i).remove(batchEndTs)
      queuedReadPrepares(phaseId)(i).remove(batchEndTs)
      commandsSent(phaseId)(i).remove(batchEndTs)
      generalLocks(phaseId)(i).unlock()
      sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, cmdsInBatch, writes, readPrepares), i)
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
