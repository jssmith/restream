package replaydb.service

import java.util.concurrent.CountDownLatch

import org.jboss.netty.channel.ChannelUpstreamHandler
import replaydb.runtimedev.RuntimeStats
import replaydb.runtimedev.distributedImpl.StateCommunicationService
import replaydb.service.driver.KryoCommands

class Server(port: Int, stats: RuntimeStats) extends ServerBase(port, stats) {
  override def getHandler(): ChannelUpstreamHandler = {
    new WorkerServiceHandler(Server.this)
  }

  var batchProgressCoordinator: BatchProgressCoordinator = null
  var stateCommunicationService: StateCommunicationService = null
  var startLatch: CountDownLatch = null
  var finishLatch: CountDownLatch = null
}
