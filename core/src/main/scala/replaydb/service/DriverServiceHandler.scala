package replaydb.service

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil
import replaydb.service.driver._

class DriverServiceHandler(clientGroup: ClientGroup, runConfiguration: RunConfiguration) extends ChannelInboundHandlerAdapter {

  var progressTracker = new ProgressTracker(runConfiguration)

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg.asInstanceOf[Command] match {
      case p: ProgressUpdateCommand => {
        println(p)
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
