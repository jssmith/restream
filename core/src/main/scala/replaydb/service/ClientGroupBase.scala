package replaydb.service

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, CountDownLatch}

import com.typesafe.scalalogging.Logger
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, ChannelUpstreamHandler, ChannelFuture}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.{Slf4JLoggerFactory, InternalLoggerFactory, InternalLogLevel}
import org.slf4j.LoggerFactory
import replaydb.service.driver._

import scala.collection.mutable.ArrayBuffer

abstract class ClientGroupBase(runConfiguration: RunConfiguration) {
  InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)
  val logger = Logger(LoggerFactory.getLogger(classOf[ClientGroupBase]))
  val cf = new ArrayBuffer[ChannelFuture]()
  val progressTracker = new ProgressTracker(runConfiguration)
  var workLatch: CountDownLatch = _

  def getHandler(): ChannelUpstreamHandler

  // TODO Synchronization is necessary to make sure two threads don't write
  // to the same channel at the same time, but right now only one thing can be writing
  // to *any* channel at a time - the synchronization should be moved to a per-channel
  // basis rather than a per group basis

  val executor = Executors.newCachedThreadPool()
  val b = new ClientBootstrap(new NioClientSocketChannelFactory(executor, executor))
  b.setPipelineFactory(new ChannelPipelineFactory {
    override def getPipeline: ChannelPipeline = {
      val p = org.jboss.netty.channel.Channels.pipeline()
      p.addLast("Logger", new LoggingHandler(InternalLogLevel.DEBUG))
      p.addLast("KryoEncoder", new KryoCommandEncoder())

      // Decode commands received from clients
      p.addLast("KryoDecoder", new KryoCommandDecoder())
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
    this.synchronized {
      try {
        logger.info(s"started broadcast of $c")
        cf.foreach {
          _.getChannel.write(c) //.sync()
        }
        logger.info("finished broadcast")
      } catch {
        case e: Throwable => logger.error("broadcast error", e)
      }
    }
  }

  def issueCommand(i: Int, c: Command): Unit = {
    this.synchronized {
      logger.debug(s"issuing command on partition $i: ${c.toString}")
      cf(i).getChannel.write(c) //.sync()
      logger.debug(s"finished issuing command on partition $i: ${c.toString}")
    }
  }

  def closeWhenDone(isWorker: Boolean = false): Unit = {
    try {
      val closeCmd = if (isWorker) {
        new CloseWorkerCommand()
      } else {
        workLatch.await()
        new CloseCommand()
      }
      // Netty complains if we `sync` within an IO thread
      val t = new Thread() {
        override def run(): Unit = {
          this.synchronized {
            cf.foreach {
              _.getChannel.write(closeCmd)
            }
            cf.foreach {
              _.getChannel.getCloseFuture.sync()
            }
          }
        }
      }
      t.start()
      t.join()
    } finally {
      b.releaseExternalResources()
    }
  }

}
