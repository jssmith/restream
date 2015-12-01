package replaydb.service

import com.typesafe.scalalogging.Logger
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory
import replaydb.service.driver._

class DriverServiceHandler(clientGroup: ClientGroup, runConfiguration: RunConfiguration) extends ChannelInboundHandlerAdapter {
  val logger = Logger(LoggerFactory.getLogger(classOf[DriverServiceHandler]))

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg.asInstanceOf[Command] match {
      case p: ProgressUpdateCommand => {
        logger.info(s"received progress update $p")
        clientGroup.progressTracker.update(p.partitionId, p.phaseId, p.finishedTimestamp) match {
          case Some(newProgressMarks) =>
            logger.info(s"have new progress marks $newProgressMarks")
            clientGroup.broadcastCommand(new UpdateAllProgressCommand(newProgressMarks))
          case None =>
            logger.info(s"no new progress to send")
        }
        if (p.done) {
          clientGroup.workLatch.countDown()
        }
      }
    }

   // TODO is this needed?
   ReferenceCountUtil.release(msg)
  }

   override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
     cause.printStackTrace()
     ctx.close()
   }
 }
