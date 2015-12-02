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
        if (p.done) {
          clientGroup.workLatch.countDown()
        } else {
          clientGroup.progressTracker.update(p.partitionId, p.phaseId, p.finishedTimestamp) match {
            case Some(newProgressMarks) =>
              logger.info(s"have new progress marks $newProgressMarks")
              clientGroup.broadcastCommand(new UpdateAllProgressCommand(newProgressMarks))
            case None =>
              logger.info(s"no new progress to send")
          }
        }
      }
    }

   // TODO is this needed?
   ReferenceCountUtil.release(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logger.error("driver error", cause)
    ctx.close()
  }

  // Additional logging code can be useful
  /*
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel read complete")
    ctx.fireChannelReadComplete()
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel active")
    ctx.fireChannelActive()
  }

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel unregistered")
    ctx.fireChannelUnregistered()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel inactive")
    ctx.fireChannelInactive()
  }

  override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel writability changed")
    ctx.fireChannelWritabilityChanged()
  }

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit = {
    logger.debug("user event triggered")
    ctx.fireUserEventTriggered()
  }

  override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
    logger.debug("channel registered")
    ctx.fireChannelRegistered()
  }
  */
}
