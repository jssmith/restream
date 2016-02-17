package replaydb.service

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.Logger
import org.jboss.netty.channel._
import org.slf4j.LoggerFactory
import replaydb.runtimedev.distributedImpl._
import replaydb.runtimedev.threadedImpl.MultiReaderEventSource
import replaydb.runtimedev.{HasRuntimeInterface, PrintSpamCounter}
import replaydb.service.driver._
import replaydb.util.PerfLogger

class WorkerServiceHandler(server: Server) extends SimpleChannelUpstreamHandler {
  val logger = Logger(LoggerFactory.getLogger(classOf[WorkerServiceHandler]))

  override def messageReceived(ctx: ChannelHandlerContext, msg: MessageEvent): Unit = {
    logger.debug(s"received message $msg")
    val startTime = System.currentTimeMillis
    msg.getMessage.asInstanceOf[Command] match {
      case c : InitReplayCommand[_] => {
        if (server.stateCommunicationService != null) {
          server.stateCommunicationService.close()
        }
        server.stateCommunicationService = new StateCommunicationService(c.workerId, c.partitionMaps.size, c.runConfiguration)
        server.networkStats.reset()
        server.garbageCollectorStats.reset()
        val constructor = Class.forName(c.programClass).getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
        val runConfig = c.runConfiguration
        val stateFactory = if (runConfig.partitioned) {
          new ReplayStateFactory(server.stateCommunicationService)
        } else {
          new ReplayStateFactoryRandomPartition(server.stateCommunicationService)
        }
        val program = constructor.newInstance(stateFactory)
        val runtime = program.asInstanceOf[HasRuntimeInterface].getRuntimeInterface
        server.batchProgressCoordinator = new BatchProgressCoordinator(runConfig.startTimestamp, runConfig.batchTimeInterval, c.workerId)

        // TODO these two probably need to change
        server.startLatch = new CountDownLatch(runConfig.numPhases * c.partitionMaps.size)
        server.finishLatch = new CountDownLatch(runConfig.hosts.length) // number of workers

        val progressSendLock = new ReentrantLock()

        val workerId = c.workerId
        logger.info(s"launching threads... number of phases ${runConfig.numPhases}")
        val threads =
          (for ((partitionId, filename) <- c.partitionMaps) yield {
            val readerThread = new MultiReaderEventSource(filename, runtime.numPhases,
              bufferSize = (ProgressTracker.FirstPhaseBatchExtraAllowance + runtime.numPhases) * runConfig.approxBatchSize * 2)
            val partitionThreads = for (phaseId <- 0 until runtime.numPhases) yield {
              new Thread(s"replay-$partitionId-$phaseId") {
                val progressCoordinator = server.batchProgressCoordinator.getCoordinator(partitionId, phaseId)

                override def run(): Unit = {
                  try {
                    server.startLatch.countDown()
                    logger.info(s"starting replay on partition $partitionId (phase $phaseId)")
                    var batchEndTimestamp = c.runConfiguration.startTimestamp
                    var lastTimestamp = Long.MinValue
                    var ct = 0
                    def sendProgress(done: Boolean = false): Unit = {
                      val progressUpdateCommand = new ProgressUpdateCommand(partitionId, phaseId, ct, batchEndTimestamp, done)
                      logger.debug(s"preparing progress update $progressUpdateCommand)")
                      progressSendLock.lock()
                      logger.debug(s"sending progress update $progressUpdateCommand)")
                      msg.getChannel.write(progressUpdateCommand) //.sync()
                      logger.debug(s"finished sending progress update")
                      progressSendLock.unlock()
                    }
                    val progressLogInterval = runConfig.batchTimeInterval / 10
                    var nextProgressTimestamp = batchEndTimestamp + progressLogInterval
                    PerfLogger.logBatchTiming(s"$partitionId $phaseId $batchEndTimestamp ${System.currentTimeMillis} START")
                    readerThread.readEvents(e => {
                      if (e.ts >= nextProgressTimestamp) {
                        PerfLogger.logBatchTiming(s"$partitionId $phaseId ${e.ts} ${System.currentTimeMillis} PROGRESS")
                        nextProgressTimestamp += progressLogInterval
                      }
                      if (e.ts >= batchEndTimestamp) {
                        logger.info(s"reached batch end on partition $partitionId phase $phaseId")
                        // Send out StateUpdateCommands
                        server.stateCommunicationService.finalizeBatch(phaseId, batchEndTimestamp)
                        sendProgress()
                        PerfLogger.logBatchTiming(s"$partitionId $phaseId $batchEndTimestamp ${System.currentTimeMillis} END")
                        batchEndTimestamp += runConfig.batchTimeInterval
                        logger.info(s"advancing endtime $batchEndTimestamp on partition $partitionId phase $phaseId")
                        progressCoordinator.awaitAdvance(batchEndTimestamp)
                        PerfLogger.logBatchTiming(s"$partitionId $phaseId $batchEndTimestamp ${System.currentTimeMillis} BEGIN_READ_WAIT")
                        server.stateCommunicationService.awaitReadsReady(phaseId, batchEndTimestamp)
                        PerfLogger.logBatchTiming(s"$partitionId $phaseId $batchEndTimestamp ${System.currentTimeMillis} START")
                      }
                      lastTimestamp = e.ts
                      runtime.update(e)(progressCoordinator)
                      ct += 1
                    })
                    PerfLogger.logBatchTiming(s"$partitionId $phaseId $batchEndTimestamp ${System.currentTimeMillis} END")
                    // TODO ETK need a better way to extract info than this - two notes on that
                    // 1. We need a way to access the state from the Stats object in general, some way
                    //    to get info from our collections at the end of the run
                    // 2. In addition to that, we should implement something like a Spark accumulator
                    //    that only accepts cumulative/associative operations but operates very efficiently
                    //    and can only be read at the end of the run
                    runtime.update(PrintSpamCounter(batchEndTimestamp - 1))(progressCoordinator)
                    server.stateCommunicationService.finalizeBatch(phaseId, batchEndTimestamp)
                    logger.info(s"finished phase $phaseId on partition $partitionId")
                    // Send progress for all phases, but only set done flag when the last phase is done
                    sendProgress(phaseId == runtime.numPhases - 1)
                  } catch {
                    case e: Throwable => logger.error("server execution error", e)
                  } finally {
                    // Print out performance statistics
                    val threadId = Thread.currentThread().getId
                    val threadMxBean = ManagementFactory.getThreadMXBean
                    val threadCpuTime = threadMxBean.getCurrentThreadCpuTime
                    val elapsedThreadCpuTime = threadCpuTime
                    PerfLogger.logCPU(s"thread $threadId (phase $phaseId) finished with elapsed cpu time ${elapsedThreadCpuTime / 1000000} ms")
                  }
                }
              }
            }
            readerThread.start()
            partitionThreads
          }).reduce(_ ++ _)
        threads.foreach(_.start())
      }

      case uap: UpdateAllProgressCommand => {
        logger.info(s"have new progress marks ${uap.progressMarks}")
        while (server.startLatch == null) { Thread.`yield`() } // just busy wait, shouldn't last long
        server.startLatch.await()
        for ((phaseId, maxTimestamp) <- uap.progressMarks) {
          server.batchProgressCoordinator.update(phaseId, maxTimestamp)
        }
      }

      case srp: StateRequestResponse => {
        server.stateCommunicationService.handleStateRequestResponse(srp)
      }

      case suc: StateUpdateCommand => {
        while (server.startLatch == null) { Thread.`yield`() } // just busy wait, shouldn't last long
        server.startLatch.await()

        server.stateCommunicationService.handleStateUpdateCommand(suc)
      }

      case _ : CloseWorkerCommand => {
        logger.info("received worker close command")
        msg.getChannel.close()
        server.finishLatch.countDown()
      }

      case _ : CloseCommand => {
        logger.info("received driver close command")
        PerfLogger.logNetwork(s"server network stats ${server.networkStats}")
        PerfLogger.logCPU(s"garbage collector stats ${server.garbageCollectorStats}")
        PerfLogger.logNetwork(s"Network Read/Write Traffic: ${server.stateCommunicationService.getReadWriteTrafficString}")
        server.stateCommunicationService.close()
        server.finishLatch.countDown()
        server.finishLatch.await()
        msg.getChannel.close()
        server.stateCommunicationService = null
        server.batchProgressCoordinator = null
        server.startLatch = null
        server.logPerformance()
      }
    }
    PerfLogger.logNetwork(s"END_HANDLER $startTime ${System.currentTimeMillis}")
  }

   override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent): Unit = {
     logger.error("worker error", event.getCause)
     event.getChannel.close()
   }
 }
