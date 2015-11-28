package replaydb.service

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.slf4j.LoggerFactory
import replaydb.service.driver.{Command, KryoCommands}

class Server(port: Int) {
  val logger = LoggerFactory.getLogger(classOf[Server])
  var f: ChannelFuture = _

  def run(): Unit = {
    logger.info("starting server on port {}", port)
    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup()
    try {
      val b = new ServerBootstrap()

      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 100)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            val p = ch.pipeline()
            p.addLast(new KryoCommandEncoder())
            p.addLast(new LengthFieldBasedFrameDecoder(KryoCommands.MAX_KRYO_MESSAGE_SIZE, 0, 4, 0, 4))
            p.addLast(new KryoCommandDecoder())
            p.addLast(new WorkerServiceHandler(Server.this))
          }
        })
      f = b.bind(port).sync()
      f.channel().closeFuture().sync()
    } finally {
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }
  }

  def write(c: Command): Unit = {
    f.channel().writeAndFlush(c).sync()
  }
}
