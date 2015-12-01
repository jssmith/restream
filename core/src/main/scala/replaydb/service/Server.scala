package replaydb.service

import io.netty.channel.ChannelInboundHandler

class Server(port: Int) extends ServerBase(port) {
  override def getHandler(): ChannelInboundHandler = {
    new WorkerServiceHandler(Server.this)
  }
}
