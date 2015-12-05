package replaydb.service

import com.typesafe.scalalogging.Logger
import org.jboss.netty.channel.{MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}
import org.slf4j.LoggerFactory
import replaydb.service.driver._

class DriverServiceHandler(clientGroup: ClientGroup, runConfiguration: RunConfiguration) extends SimpleChannelUpstreamHandler {
  val logger = Logger(LoggerFactory.getLogger(classOf[DriverServiceHandler]))

  override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
    me.getMessage.asInstanceOf[Command] match {
      case p: ProgressUpdateCommand => {
        logger.info(s"received progress update $p")
        if (p.done) {
          clientGroup.workLatch.countDown()
        } else {
          clientGroup.progressTracker.synchronized {
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
    }

   // TODO is this needed? - not supported in Netty 3 so hopefully not...
//   ReferenceCountUtil.release(msg)
  }

  // Leaving this commented out for now because otherwise the program will continue
  // once one thread has an exception, easier to debug if everything dies after an
  // exception occurs
//  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent): Unit = {
//    logger.error("driver error", event.getCause)
//    event.getChannel.close()
//  }

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
