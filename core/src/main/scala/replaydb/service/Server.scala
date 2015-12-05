package replaydb.service

import java.util.concurrent.CountDownLatch

import org.jboss.netty.channel.ChannelUpstreamHandler
import replaydb.runtimedev.distributedImpl.StateCommunicationService

class Server(port: Int) extends ServerBase(port) {
  override def getHandler(): ChannelUpstreamHandler = {
    new WorkerServiceHandler(Server.this)
  }

  var batchProgressCoordinator: BatchProgressCoordinator = null
  var stateCommunicationService: StateCommunicationService = null
  var startLatch: CountDownLatch = null
  var finishLatch: CountDownLatch = null
}
