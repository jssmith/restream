package replaydb.service

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.Logger
import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import replaydb.service.driver._

import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

class ClientGroup(runConfiguration: RunConfiguration) {
  val logger = Logger(LoggerFactory.getLogger(classOf[ClientGroup]))
  val group = new NioEventLoopGroup()
  val cf = new ArrayBuffer[ChannelFuture]()
  val progressTracker = new ProgressTracker(runConfiguration)
  var workLatch: CountDownLatch = _

  val b = new Bootstrap()
  b.group(group)
    .channel(classOf[NioSocketChannel])
    .option(ChannelOption.TCP_NODELAY.asInstanceOf[ChannelOption[Any]], true)
    .handler(new LoggingHandler(LogLevel.DEBUG))
    .handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        val p = ch.pipeline()
        // Encode commands sent to clients
        p.addLast(new KryoCommandEncoder())

        // Decode commands received from clients
        p.addLast(new LengthFieldBasedFrameDecoder(KryoCommands.MAX_KRYO_MESSAGE_SIZE, 0, 4, 0, 4))
        p.addLast(new KryoCommandDecoder())
        p.addLast(new DriverServiceHandler(ClientGroup.this, runConfiguration))
      }
    })

  def connect(hostConfigurations: Iterable[Hosts.HostConfiguration]): Unit = {
    logger.info(s"connecting with host configuration ${hostConfigurations.mkString(",")}")
    workLatch = new CountDownLatch(runConfiguration.numPartitions)
    for (hostConfiguration <- hostConfigurations) {
      cf += b.connect(hostConfiguration.host, hostConfiguration.port).sync()
    }
  }

  def broadcastCommand(c: Command): Unit = {
    cf.foreach { _.channel().writeAndFlush(c).sync() }
  }

  def issueCommand(i: Int, c: Command): Unit = {
    logger.info(s"issuing command on partition $i: ${c.toString}")
    // TODO - do we need to sync here?
    cf(i).channel().writeAndFlush(c).sync()
  }

  def closeWhenDone(): Unit = {
    try {
      workLatch.await()
      cf.foreach { _.channel().writeAndFlush(new CloseCommand) }
      cf.foreach { _.channel().closeFuture().sync() }
    } finally {
      group.shutdownGracefully()
    }
  }

}
