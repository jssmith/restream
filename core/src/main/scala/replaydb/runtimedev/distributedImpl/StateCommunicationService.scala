package replaydb.runtimedev.distributedImpl

import java.util.concurrent.locks.{Lock, Condition, ReentrantLock}

import com.typesafe.scalalogging.Logger
import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import replaydb.runtimedev.ReplayState
import replaydb.service.driver.{RunConfiguration, Command}
import replaydb.service.{ProgressTracker, ClientGroupBase}
import replaydb.util.PerfLogger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class StateCommunicationService(workerId: Int, numLocalPartitions: Int, runConfiguration: RunConfiguration) {
  var readRequestsSatisfiedLocally = 0
  var readRequestsSatisfiedRemotely = 0
  var writesSentLocally = 0
  var writesSentRemotely = 0

  // TODO ETK this should just be always enabled; leaving as config for now for debugging purposes
  val waitAtBatchBoundary = true // runConfiguration.waitAtBatchBoundary

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
  connectToClients()

  def connectToClients(): Unit = {
    client.connect(runConfiguration.hosts.zipWithIndex.filter(_._2 != workerId).map(_._1))
  }

  def close(): Unit = {
    client.closeWhenDone(true)
  }

  def issueCommandToWorker(wId: Int, cmd: Command): Unit = {
    // subtract 1 since the local partition isn't in the array
    client.issueCommand(if (wId > workerId) wId - 1 else wId, cmd)
  }

  // State is owned by worker #: partitionFn(key) % workerCount
  val states: ArrayBuffer[ReplayState with Partitioned] = ArrayBuffer()

  val commandsReceived: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)
  val cmdsInBatch: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)

  val outstandingReads: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)

  val commandsSent: Array[Array[mutable.Map[Long, Int]]] = Array.ofDim(numPhases, numWorkers)
  val partitionsFinished: Array[mutable.Map[Long, Int]] = Array.ofDim(numPhases)

  val outstandingReadsLocks: Array[Array[Lock]] = Array.ofDim(numPhases, numWorkers)
  val outstandingReadsConditions: Array[Array[Condition]] = Array.ofDim(numPhases, numWorkers)
  val commandsReceivedLocks: Array[Lock] = Array.ofDim(numPhases)
  val partitionsFinishedLocks: Array[Lock] = Array.ofDim(numPhases)

  for (i <- 0 until numPhases; j <- 0 until numWorkers) {
    commandsReceived(i)(j) = mutable.Map()
    commandsSent(i)(j) = mutable.Map()
    cmdsInBatch(i)(j) = mutable.Map()
    outstandingReads(i)(j) = mutable.Map()

    outstandingReadsLocks(i)(j) = new ReentrantLock()
    outstandingReadsConditions(i)(j) = outstandingReadsLocks(i)(j).newCondition()
    if (j == 0) {
      commandsReceivedLocks(i) = new ReentrantLock()
      partitionsFinishedLocks(i) = new ReentrantLock()
      partitionsFinished(i) = mutable.Map()
    }
  }

  // Handle a StateRequestResponse:
  //  - Place the newly acquired values into local ReplayStates
  //  - Updates the number of reads we've received so far for this phase/batch
  //  - If we've received all of the reads, notify anyone waiting for that condition
  def handleStateRequestResponse(resp: StateRequestResponse): Unit = {
    for ((stateResponses, stateId) <- resp.responses.zipWithIndex) {
      states(stateId).insertPreparedValues(resp.phaseId, resp.batchEndTs, stateResponses)
    }
    if (waitAtBatchBoundary) {
      outstandingReadsLocks(resp.phaseId+1)(workerId).lock()
      val remainingCount = outstandingReads(resp.phaseId+1)(workerId).getOrElse(resp.batchEndTs, 0) -
        resp.responses.map(_.length).sum
      outstandingReads(resp.phaseId+1)(workerId)(resp.batchEndTs) = remainingCount
      if (remainingCount == 0) {
        outstandingReadsConditions(resp.phaseId+1)(workerId).signalAll()
      }
      outstandingReadsLocks(resp.phaseId+1)(workerId).unlock()
    }
  }

  // Handles a StateUpdateCommand:
  //  - Puts all the remote writes into local ReplayStates
  //  - Buffers all of the remote readPrepares into ReplayStates
  //  - Increments the number of commands we've received for this phase/batch
  //  - If we've received all commands from all workers, fulfill the readPrepares and respond to requesters
  def handleStateUpdateCommand(cmd: StateUpdateCommand): Unit = {
    val phaseId = cmd.phaseId
    val origWorker = cmd.originatingWorkerId
    val batchEndTs = cmd.batchEndTs
    logger.info(s"Phase $workerId-$phaseId received writes from $origWorker for batch $batchEndTs")

    for ((writes, stateId) <- cmd.writes.zipWithIndex) {
      states(stateId).insertRemoteWrites(writes)
    }
    for ((readPrepares, stateId) <- cmd.readPrepares.zipWithIndex) {
      states(stateId).bufferRemoteReadPrepares(phaseId, origWorker, batchEndTs, readPrepares)
    }

    // Check if we've received all of the commands that all workers are going to send to us
    commandsReceivedLocks(phaseId).lock()
    commandsReceived(phaseId)(origWorker)(batchEndTs) =
      commandsReceived(phaseId)(origWorker).getOrElse(batchEndTs, 0) + 1
    // At least one command is marked with cmdsInBatch > 0; any with <= 0 are ignored
    // Note this means that no command may have a positive value which is incorrect
    if (cmd.cmdsInBatch > 0) {
      cmdsInBatch(phaseId)(origWorker)(batchEndTs) = cmd.cmdsInBatch
    }
    // If we've received all commands from all other workers then we're good
    val finalize = (0 until numWorkers).forall(batchHasReceivedAllCommands(phaseId, _, batchEndTs))
    // Clean up the old counts
    if (finalize) {
      for (i <- 0 until numWorkers) {
        cmdsInBatch(phaseId)(i).remove(batchEndTs)
        commandsReceived(phaseId)(i).remove(batchEndTs)
      }
    }
    commandsReceivedLocks(phaseId).unlock()

    if (finalize) {
      //logger.info(s"Phase $workerId-$phaseId processing read requests for ${readRequestsToProcess.get.size} partitions")
      processStateReadRequests(phaseId, batchEndTs)
      // at this point, no more reads will be coming for this specific phase/batch...
      if (phaseId == numPhases - 1) {
        PerfLogger.logGc("Custom GC stats: (total merged values in collection, total unmerged values, number of ReplayValues, GC'd values)"
          + (for ((s, id) <- states.zipWithIndex) yield {
          s"State ID $id: ${s.gcOlderThan(batchEndTs)}"
        }).mkString(", "))
      }
    }
  }

  def batchHasReceivedAllCommands(phaseId: Int, worker: Int, batchEndTs: Long): Boolean = {
    cmdsInBatch(phaseId)(worker).contains(batchEndTs) &&
      cmdsInBatch(phaseId)(worker)(batchEndTs) == commandsReceived(phaseId)(worker).getOrElse(batchEndTs, 0)
  }

  // Process all of the readRequests for this phase/batch and send them back to the requesters
  // MUST only be called after all of the writes are present for this phase/batch
  def processStateReadRequests(phaseId: Int, batchEndTs: Long): Unit = {
    for (i <- 0 until numWorkers) {
      val allResponses = states.map(_.fulfillRemoteReadPrepare(phaseId, i, batchEndTs)).toArray
      for (responses <- StateCommunicationService.splitIntoMessages(allResponses)) {
        val respCmd = StateRequestResponse(phaseId, batchEndTs, responses)
        if (i == workerId) {
          handleStateRequestResponse(respCmd)
        } else {
          issueCommandToWorker(i, respCmd)
        }
      }
    }
  }

  // Wait until all of the reads are available locally (BLOCKS)
  def awaitReadsReady(phaseId: Int, batchEndTs: Long): Unit = {
    if (phaseId == 0) { // Phase 0 never has to wait on reads
      return
    }
    if (waitAtBatchBoundary) {
      outstandingReadsLocks(phaseId)(workerId).lock()
      while (outstandingReads(phaseId)(workerId).getOrElse(batchEndTs, -1) != 0) {
        outstandingReadsConditions(phaseId)(workerId).await()
      }
      outstandingReads(phaseId)(workerId).remove(batchEndTs)
      outstandingReadsLocks(phaseId)(workerId).unlock()
    }
  }

  // Closes out a batch, but only if all of the partitions on this machine are finished
  //  - Sends out all of the outstanding writes and readPrepares
  //  - Updates the count of outstandingReads (used to know when all readPrepares have been filled)
  def finalizeBatch(phaseId: Int, batchEndTs: Long): Unit = {
    logger.info(s"Phase $workerId-$phaseId finalizing batch $batchEndTs")

    partitionsFinishedLocks(phaseId).lock()
    val numFinished = partitionsFinished(phaseId).getOrElse(batchEndTs, 0) + 1
    if (numFinished == 1) {
      states.foreach(_.prepareForBatch(phaseId,
        batchEndTs + (2 + numPhases + ProgressTracker.FirstPhaseBatchExtraAllowance) * batchTimeInterval))
    }
    if (numFinished < numLocalPartitions) {
      partitionsFinished(phaseId)(batchEndTs) = numFinished
      partitionsFinishedLocks(phaseId).unlock()
      return
    }
    partitionsFinished(phaseId).remove(batchEndTs)
    partitionsFinishedLocks(phaseId).unlock()

    if (phaseId == numPhases - 1) {
      states.foreach(_.cleanupBatch(phaseId, batchEndTs))
      return // nothing to be done - final phase can't have any readPrepares or writes
    }

    for (i <- 0 until numWorkers) {
      val allWrites = states.map(_.getAndClearWrites(phaseId, i, batchEndTs)).toArray
      val writeChunks = StateCommunicationService.splitIntoMessages(allWrites)
      val allReadPrepares = states.map(_.getAndClearLocalReadPrepares(phaseId, i, batchEndTs)).toArray
      val readPrepareChunks = StateCommunicationService.splitIntoMessages(allReadPrepares)

      if (waitAtBatchBoundary) {
        outstandingReadsLocks(phaseId+1)(workerId).lock()
        outstandingReads(phaseId+1)(workerId)(batchEndTs) =
          outstandingReads(phaseId+1)(workerId).getOrElse(batchEndTs, 0) + allReadPrepares.map(_.length).sum
        outstandingReadsLocks(phaseId+1)(workerId).unlock()
      }

      val cmdsInBatch = writeChunks.length + readPrepareChunks.length - 1
      for (write <- writeChunks.slice(0, writeChunks.length - 1)) {
        sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, cmdsInBatch, write, Array()), i)
      }
      for (readPrep <- readPrepareChunks.slice(0, readPrepareChunks.length - 1)) {
        sendStateUpdateCommand(StateUpdateCommand(workerId, phaseId, batchEndTs, cmdsInBatch, Array(), readPrep), i)
      }
      // This last message may be bigger than MaxMessagesPerCommand but we will allow it
      sendStateUpdateCommand(
        StateUpdateCommand(workerId, phaseId, batchEndTs, cmdsInBatch, writeChunks.last, readPrepareChunks.last), i)
    }
    states.foreach(_.cleanupBatch(phaseId, batchEndTs))
  }

  // Return the worker which should be contacted to get the given key (for reads)
  def getSourceWorker[K](key: K, partitionFn: K => List[Int]): Int = {
    val srcWorkers = partitionFn(key).map((i: Int) => (i & 0x7FFFFFFF) % numWorkers)
    // If it's available locally, grab from local, else grab from a random partition
    srcWorkers.find(_ == workerId) match {
      case Some(id) => id
      case None => srcWorkers(Random.nextInt(srcWorkers.size))
    }
  }

  // Return the worker(s) which this key should live on (for writes)
  def getDestWorkers[K](key: K, partitionFn: K => List[Int]): List[Int] = {
    partitionFn(key).map((i: Int) => (i & 0x7FFFFFFF) % numWorkers).distinct
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

  def registerReplayState(collectionId: Int, rs: ReplayState with Partitioned): Unit = {
    if (states.length != collectionId) {
      throw new IllegalStateException("collectionId must be equal to (previous number of ReplayStates added)")
    }
    states += rs
    for (pid <- 0 until numPhases; batchNumber <- 0 until (2 + numPhases + ProgressTracker.FirstPhaseBatchExtraAllowance)) {
      rs.prepareForBatch(pid, runConfiguration.startTimestamp + batchNumber * batchTimeInterval)
    }
  }

  def getReadWriteTrafficString: String = {
    s"Local Reads: $readRequestsSatisfiedLocally / Remote Reads $readRequestsSatisfiedRemotely // " +
      s"Local Writes $writesSentLocally / Remote Writes $writesSentRemotely"
  }

  def batchTimeInterval: Long = runConfiguration.batchTimeInterval

}

object StateCommunicationService {
  val MaxMessagesPerCommand = 50000

  trait StateRead {
    def ts: Long
  }

  trait StateResponse {
    def ts: Long
    def value: Any
  }

  trait StateWrite {
    def ts: Long
    def merge: Any
  }

  def splitIntoMessages[T:ClassTag](input: Array[Array[T]]): Array[Array[Array[T]]] = {
    val result = ArrayBuffer[Array[Array[T]]]()
    var totalAccum = 0
    var outerIdx = 0
    var innerIdx = 0
    val total = input.map(_.length).sum
    if (total == 0) {
      return Array.ofDim[T](1, input.length, 0)
    }
    while (totalAccum < total) {
      var accum = 0
      val temp: Array[ArrayBuffer[T]] = Array.ofDim[ArrayBuffer[T]](input.length).map(_ => ArrayBuffer[T]())
      while (accum < MaxMessagesPerCommand && totalAccum < total) {
        if (accum + (input(outerIdx).length - innerIdx) >= MaxMessagesPerCommand) {
          temp(outerIdx) ++= input(outerIdx).slice(innerIdx, MaxMessagesPerCommand - accum + innerIdx)
          innerIdx = MaxMessagesPerCommand - accum + innerIdx
          totalAccum += MaxMessagesPerCommand - accum
          accum = MaxMessagesPerCommand
          if (innerIdx == input(outerIdx).length) {
            outerIdx += 1
            innerIdx = 0
          }
        } else if (totalAccum + (input(outerIdx).length - innerIdx) >= total) {
          temp(outerIdx) ++= input(outerIdx).slice(innerIdx, input(outerIdx).length)
          totalAccum += input(outerIdx).length - innerIdx
          accum = MaxMessagesPerCommand
        } else {
          temp(outerIdx) ++= input(outerIdx).slice(innerIdx, input(outerIdx).length)
          accum += input(outerIdx).length - innerIdx
          totalAccum += input(outerIdx).length - innerIdx
          innerIdx = 0
          outerIdx += 1
        }
      }
      result += temp.map(_.toArray[T])
    }
    result.toArray
  }
}
