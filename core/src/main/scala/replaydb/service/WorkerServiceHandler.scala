package replaydb.service

import java.io.FileInputStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import org.jboss.netty.channel._

import scala.language.reflectiveCalls // TODO

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev.{PrintSpamCounter, CoordinatorInterface, HasRuntimeInterface}
import replaydb.runtimedev.distributedImpl._
import replaydb.service.driver._


class WorkerServiceHandler(server: Server) extends SimpleChannelUpstreamHandler {
  val logger = Logger(LoggerFactory.getLogger(classOf[WorkerServiceHandler]))

  override def messageReceived(ctx: ChannelHandlerContext, msg: MessageEvent): Unit = {
    logger.info(s"received message $msg")
    msg.getMessage.asInstanceOf[Command] match {
      case c : InitReplayCommand[_] => {
        if (server.stateCommunicationService != null) {
          server.stateCommunicationService.close()
        }
        server.stateCommunicationService = new StateCommunicationService(c.hostId, c.runConfiguration)
        val stateFactory = new ReplayStateFactory(server.stateCommunicationService)
        val constructor = Class.forName(c.programClass).getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
        val runConfig = c.runConfiguration
        val program = constructor.newInstance(stateFactory)
        val runtime = program.asInstanceOf[HasRuntimeInterface].getRuntimeInterface
        server.batchProgressCoordinator = new BatchProgressCoordinator(runConfig.startTimestamp, runConfig.batchTimeInterval)
        server.startLatch = new CountDownLatch(runConfig.numPhases)
        server.finishLatch = new CountDownLatch(runConfig.numPartitions)

        val progressSendLock = new ReentrantLock()

        val partitionId = c.partitionId
        logger.info(s"launching threads... number of phases ${runConfig.numPhases}")
        val threads =
          for (phaseId <- 1 to runtime.numPhases) yield {
            new Thread(s"replay-$partitionId-$phaseId") {
              val progressCoordinator = server.batchProgressCoordinator.getCoordinator(phaseId)
              // TODO
              implicit val coordinator = new CoordinatorInterface(partitionId, phaseId) {

                var bets: Long = 0

                def setBatchEndTs(ts: Long): Unit = {
                  bets = ts
                }

                override def batchEndTs: Long = bets

                override def gcAllReplayState(): Unit = ???

                // TODO should only have one of these two
                override def batchId: Int = ???

                override def reportCheckpoint(ts: Long, ct: Long): (Long, Long) = ???

                override def reportFinished(): Unit = ???
              }
              override def run(): Unit = {
                // TODO
                //  - separate reader thread
                try {
                  server.startLatch.countDown()

                  logger.info(s"starting replay on partition $partitionId (phase $phaseId)")
                  var batchEndTimestamp = c.runConfiguration.startTimestamp
                  val eventStorage = new SocialNetworkStorage
                  var lastTimestamp = Long.MinValue
                  var ct = 0
                  def sendProgress(done: Boolean = false): Unit = {
                    val progressUpdateCommand = new ProgressUpdateCommand(partitionId, phaseId, ct, batchEndTimestamp, done)
                    logger.info(s"preparing progress update $progressUpdateCommand)")
                    progressSendLock.lock()
                    logger.info(s"sending progress update $progressUpdateCommand)")
                    msg.getChannel.write(progressUpdateCommand) //.sync()
                    logger.debug(s"finished sending progress update")
                    progressSendLock.unlock()
                  }
                  eventStorage.readEvents(new FileInputStream(c.filename), e => {
                    if (e.ts >= batchEndTimestamp) {
                      logger.info(s"reached batch end on partition $partitionId phase $phaseId")
                      // Send out StateUpdateCommands
                      server.stateCommunicationService.finalizeBatch(phaseId, batchEndTimestamp)
                      sendProgress()
                      batchEndTimestamp += runConfig.batchTimeInterval
                      coordinator.setBatchEndTs(batchEndTimestamp)
                      logger.info(s"advancing endtime $batchEndTimestamp on partition $partitionId phase $phaseId")
                      progressCoordinator.awaitAdvance(batchEndTimestamp)
                    }
                    lastTimestamp = e.ts
                    runtime.update(e)
                    ct += 1
                  })
                  // TODO need a better way to extract info than this
                  runtime.update(PrintSpamCounter(batchEndTimestamp - 1))
                  server.stateCommunicationService.finalizeBatch(phaseId, batchEndTimestamp)
                  logger.info(s"finished phase $phaseId on partition $partitionId")
                  // Send progress for all phases, but only set done flag when the last phase is done
                  sendProgress(phaseId == runtime.numPhases)
                } catch {
                  case e: Exception => e.printStackTrace()
                  case e: Throwable => e.printStackTrace()
                }
              }
            }
          }
        threads.foreach(_.start())
//        threads.foreach(_.join())
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
        msg.getChannel.close()
        server.finishLatch.countDown()
      }

      case _ : CloseCommand => {
        server.stateCommunicationService.close()
        server.finishLatch.countDown()
        server.finishLatch.await()
        msg.getChannel.close()
        server.stateCommunicationService = null
        server.batchProgressCoordinator = null
        server.startLatch = null
      }
    }
    // TODO is this needed?
//    ReferenceCountUtil.release(msg)
  }

  // TODO leaving commented out for now so that the whole system will die
  // if any IO thread throws an exception; easier to debug
//   override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent): Unit = {
//     event.getCause.printStackTrace()
//     event.getChannel.close()
//   }
 }
