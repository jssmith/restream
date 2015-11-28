package replaydb.service

import java.io.FileInputStream

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev.{HasRuntimeInterface, ReplayState}
import replaydb.runtimedev.distributedImpl.ReplayStateFactory
import replaydb.service.driver._
import org.slf4j.LoggerFactory


class WorkerServiceHandler(server: Server) extends ChannelInboundHandlerAdapter {
  val logger = LoggerFactory.getLogger(classOf[WorkerServiceHandler])
  val stateFactory = new ReplayStateFactory
  val batchProgressCoordinator = new BatchProgressCoordinator()

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    logger.info("received message {}", msg.toString)
    msg.asInstanceOf[Command] match {
      case c : InitReplayCommand[_] => {
        val constructor = Class.forName(c.programClass).getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
        val program = constructor.newInstance(stateFactory)
        val runtime = program.asInstanceOf[HasRuntimeInterface].getRuntimeInterface
        var deltaMap = Map[ReplayState, ReplayState]()

        val threads =
          for ((partitionId, partitionFn) <- c.files; phaseId <- 1 to runtime.numPhases) yield {
            new Thread(s"replay-$partitionId-$phaseId") {
              val progressCoordinator = batchProgressCoordinator.getCoordinator(partitionId, phaseId)
              override def run(): Unit = {
                // TODO
                //  - separate reader thread
                //  - barrier to prevent from running until previous batch has finished
                try {
                  logger.info("starting replay on partition {}, will update at interval {}", partitionId.toInt, c.progressUpdateInterval)
                  var batchEndTimestamp = c.startTimestamp
                  val eventStorage = new SocialNetworkStorage
                  var lastTimestamp = Long.MinValue
                  var ct = 0
                  def sendProgress(done: Boolean = false): Unit = {
                    // TODO not sure whether this write is thread-safe
                    ctx.writeAndFlush(new ProgressUpdateCommand(partitionId, 0, ct, lastTimestamp - 1, false)).sync()
                  }
                  eventStorage.readEvents(new FileInputStream(partitionFn), e => {
                    if (e.ts >= batchEndTimestamp) {
                      sendProgress()
                      batchEndTimestamp += c.batchTimeInterval
                      progressCoordinator.awaitAdvance(batchEndTimestamp)
                    }
                    lastTimestamp = e.ts
                    runtime.update(partitionId, phaseId, e, null)
                    ct += 1
                    if (ct % c.progressUpdateInterval == 0) {
                      logger.info("progress on partition {}: {} {}", new Integer(partitionId), new Integer(ct), new java.lang.Long(lastTimestamp - 1))
                      sendProgress()
                    }
                  })
                  logger.info("finished on partition {}", partitionId)
                  sendProgress(true)
                } catch {
                  case e: Exception => e.printStackTrace()
                  case e: Throwable => e.printStackTrace()
                }
              }
            }
          }
        threads.foreach(_.start())
        threads.foreach(_.join())
      }

      case uap: UpdateAllProgressCommand => {
        for ((phaseId, maxTimestamp) <- uap.progressMarks) {
          batchProgressCoordinator.update(phaseId, maxTimestamp)
        }
      }

      case _ : CloseCommand => ctx.close()
    }
    // TODO is this needed?
    ReferenceCountUtil.release(msg)
  }

   override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
     cause.printStackTrace()
     ctx.close()
   }
 }
