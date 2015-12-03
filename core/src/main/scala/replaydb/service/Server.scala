package replaydb.service

import java.util.concurrent.CountDownLatch

import io.netty.channel.ChannelHandler
import replaydb.runtimedev.distributedImpl.StateCommunicationService

class Server(port: Int) extends ServerBase(port) {
  override def getHandler(): ChannelHandler = {
    new WorkerServiceHandler(Server.this)
  }

  var batchProgressCoordinator: BatchProgressCoordinator = null
  var stateCommunicationService: StateCommunicationService = null
  var startLatch: CountDownLatch = null
  var finishLatch: CountDownLatch = null
}
