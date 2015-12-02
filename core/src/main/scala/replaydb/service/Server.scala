package replaydb.service

import java.util.concurrent.CountDownLatch

import io.netty.channel.ChannelInboundHandler
import replaydb.runtimedev.distributedImpl.StateCommunicationService

class Server(port: Int) extends ServerBase(port) {
  override def getHandler(): ChannelInboundHandler = {
    new WorkerServiceHandler(Server.this)
  }

  var batchProgressCoordinator: BatchProgressCoordinator = null
  var stateCommunicationService: StateCommunicationService = null

  val startLatch = new CountDownLatch(5) // TODO this should be numPhases
}
