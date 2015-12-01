package replaydb.service

import io.netty.channel.ChannelInboundHandler

trait ServiceHandlerFactory {
  def getHandler(): ChannelInboundHandler
}
