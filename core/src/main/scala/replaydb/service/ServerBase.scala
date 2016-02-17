package replaydb.service

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.typesafe.scalalogging.Logger
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.{Slf4JLoggerFactory, InternalLoggerFactory, InternalLogLevel}
import org.slf4j.LoggerFactory
import replaydb.service.driver.Command
import replaydb.util.{TimingThreadFactory, GarbageCollectorStats, PerfLogger, NetworkStats}

abstract class ServerBase(port: Int) {
  InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)
  val logger = Logger(LoggerFactory.getLogger(classOf[ServerBase]))
  val networkStats = new NetworkStats()
  val garbageCollectorStats = new GarbageCollectorStats()
  val bossExecutorFactory = new TimingThreadFactory()
  val workerExecutorFactory = new TimingThreadFactory()
  var f: Channel = _
  var closeRunnable: Runnable = _

  networkStats.reset()
  garbageCollectorStats.reset()
  garbageCollectorStats.registerNotifications(PerfLogger.logGc)

  def getHandler(): ChannelHandler

  def run(): Unit = {
    logger.info(s"starting server on port $port")
    val b = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(bossExecutorFactory),
      Executors.newCachedThreadPool(workerExecutorFactory)))
    b.setPipelineFactory(new ChannelPipelineFactory {
      override def getPipeline: ChannelPipeline = {
        val p = org.jboss.netty.channel.Channels.pipeline()
        p.addLast("Logger", new LoggingHandler(InternalLogLevel.DEBUG))
        p.addLast("KryoEncoder", new KryoCommandEncoder(networkStats))

        // Decode commands received from clients
        p.addLast("KryoDecoder", new KryoCommandDecoder(networkStats))
        p.addLast("Handler", getHandler())
        p
      }
    })
    b.setOption("tcpNoDelay", true)
//    b.setOption("")
//      .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 100)
    f = b.bind(new InetSocketAddress(port))
    closeRunnable = new Runnable() {
      override def run(): Unit = {
        try {
          f.close().sync()
        } finally {
          b.releaseExternalResources()
        }
        logPerformance()
      }
    }
  }

  def logPerformance(): Unit = {
    PerfLogger.logGc(s"Garbage collection stats: $garbageCollectorStats")
    PerfLogger.logCPU(s"boss thread pool time: ${bossExecutorFactory.getSummary() / 1000000} ms")
    PerfLogger.logCPU(s"worker thread pool time: ${workerExecutorFactory.getSummary() / 1000000} ms")
  }

  def close(): Unit = {
    closeRunnable.run()
  }

  def write(c: Command): Unit = {
    f.write(c) //.sync()
  }
}
