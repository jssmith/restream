package replaydb.service

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, CountDownLatch}

import com.typesafe.scalalogging.Logger
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, ChannelUpstreamHandler, ChannelFuture}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.{Slf4JLoggerFactory, InternalLoggerFactory, InternalLogLevel}
import org.slf4j.LoggerFactory
import replaydb.service.driver._
import replaydb.util.{PerfLogger, NetworkStats}

import scala.collection.mutable.ArrayBuffer

abstract class ClientGroupBase(runConfiguration: RunConfiguration) {
  InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)
  val logger = Logger(LoggerFactory.getLogger(classOf[ClientGroupBase]))
  val networkStats = new NetworkStats()
  val cf = new ArrayBuffer[ChannelFuture]()
  val channelLocks = new ArrayBuffer[ReentrantLock]()
  val progressTracker = new ProgressTracker(runConfiguration)
  var workLatch: CountDownLatch = _

  def getHandler(): ChannelUpstreamHandler

  val executor = Executors.newCachedThreadPool()
  val b = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
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

  def connect(hostConfigurations: Iterable[Hosts.HostConfiguration]): Unit = {
    logger.info(s"connecting with host configuration ${hostConfigurations.mkString(",")}")
    workLatch = new CountDownLatch(runConfiguration.numPartitions)
    // Netty complains if `connect`/`sync` is called in an I/O thread, so spin up another to run it
    val t = new Thread() {
      override def run(): Unit = {
        try {
          for (hostConfiguration <- hostConfigurations) {
            cf += b.connect(new InetSocketAddress(hostConfiguration.host, hostConfiguration.port)).sync()
            channelLocks += new ReentrantLock()
          }
        } catch {
          case e: Throwable => logger.error("connection error", e)
        }
      }
    }
    t.start()
    t.join()
  }

  def broadcastCommand(c: Command): Unit = {
    logger.info("Attempting to broadcast")
    try {
      logger.info(s"started broadcast of $c")
      for (i <- cf.indices) {
        channelLocks(i).lock()
        cf(i).getChannel.write(c) //.sync()
        channelLocks(i).unlock()
      }
      logger.info("finished broadcast")
    } catch {
      case e: Throwable => logger.error("broadcast error", e)
    }
  }

  def issueCommand(i: Int, c: Command, sync: Boolean = false): Unit = {
    channelLocks(i).lock()
    logger.debug(s"issuing command on partition $i: ${c.toString}")
    val f = cf(i).getChannel.write(c)
    if (sync) {
      f.sync()
    }
    logger.debug(s"finished issuing command on partition $i: ${c.toString}")
    channelLocks(i).unlock()
  }

  def closeWhenDone(isWorker: Boolean = false): Unit = {
    try {
      val closeCmd = if (isWorker) {
        new CloseWorkerCommand()
      } else {
        logger.debug("driver awaiting close in closeWhenDone")
        workLatch.await()
        logger.debug("driver finished awaiting close in closeWhenDone")
        new CloseCommand()
      }
      PerfLogger.log(s"client network stats $networkStats")
      // Netty complains if we `sync` within an IO thread
      val t = new Thread() {
        override def run(): Unit = {
          logger.debug(s"closing ${cf.indices.size} connections")
          cf.indices.foreach(issueCommand(_, closeCmd, true))
          for (i <- cf.indices) {
            channelLocks(i).lock()
            logger.debug(s"closing channel $i")
            cf(i).getChannel.close().sync()
            channelLocks(i).unlock()
            logger.debug(s"done closing channel $i")
          }
        }
      }
      t.start()
      logger.debug("awaiting closeWhenDone thread completion")
      t.join()
      logger.debug("finished awaiting closeWhenDone thread completion")
    } finally {
      b.releaseExternalResources()
    }
  }

}
