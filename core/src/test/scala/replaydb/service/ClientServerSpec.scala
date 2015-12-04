package replaydb.service

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.typesafe.scalalogging.Logger
import io.netty.channel.{ChannelHandlerAdapter, ChannelHandler, ChannelHandlerContext}
import io.netty.util.concurrent.ImmediateEventExecutor
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import replaydb.service.driver.Hosts.HostConfiguration
import replaydb.service.driver._

class ClientServerSpec extends FlatSpec {
  val logger = Logger(LoggerFactory.getLogger(classOf[ClientServerSpec]))

  "A single client and sever" should "exchange messages" in {
    logger.debug("single message tests")
    val localhost = "127.0.0.1"
    val port = 15567
    val hosts = Array(new HostConfiguration(localhost, port))
    val m = Map[Int,Long](1->1L)
    @volatile var progressReceived = false
    @volatile var closeReceived = false
    val s = new ServerBase(port) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: UpdateAllProgressCommand =>
                assert(p.progressMarks === m)
              case c: CloseCommand =>
                // shut down the server
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceived = true
                progressReceived = true
            }
          }
        }
      }
    }
    s.run()
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, hosts, 0L, 100L)
    val c = new ClientGroupBase(rc) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = ???
        }
      }
    }
    val localHostConfiguration = new HostConfiguration(localhost, port)
    c.connect(Array(localHostConfiguration))
    c.issueCommand(0, new UpdateAllProgressCommand(m), ImmediateEventExecutor.INSTANCE)
    c.issueCommand(0, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    logger.debug("close issued")
    s.close()
    assert(progressReceived)
    assert(closeReceived)
  }

  they should "exchange multiple messages" in {
    logger.debug("starting multiple messages test")
    val localhost = "127.0.0.1"
    val port = 15567
    val hosts = Array(new HostConfiguration(localhost, port))
    val m = Map[Int,Long](0 -> 15L)
    val n = 1000
    @volatile var progressReceivedCt = 0
    @volatile var closeReceived = false
    val s = new ServerBase(port) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: UpdateAllProgressCommand =>
                progressReceivedCt += 1
                assert(p.progressMarks.keys.max === progressReceivedCt)
                assert(p.progressMarks.values.sum === 25)
              case c: CloseCommand =>
                // shut down the server
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceived = true
            }
          }
        }
      }
    }
    s.run()
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, hosts, 0L, 100L)
    val c = new ClientGroupBase(rc) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = ???
        }
      }
    }
    val localHostConfiguration = new HostConfiguration(localhost, port)
    c.connect(Array(localHostConfiguration))
    for (i <- 1 to n) {
      c.issueCommand(0, new UpdateAllProgressCommand(m + (i -> 10)), ImmediateEventExecutor.INSTANCE)
    }
    c.issueCommand(0, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    logger.debug("close issued")
    s.close()
    assert(progressReceivedCt === n)
    assert(closeReceived)
  }

  they should "ping back and forth" in {
    logger.debug("starting ping test")
    val localhost = "127.0.0.1"
    val port = 15567
    val hosts = Array(new HostConfiguration(localhost, port))
    val m = Map[Int,Long](0 -> 15L)
    val n = 10
    @volatile var progressReceivedCt = 0
    @volatile var sumResponseCt = 0
    @volatile var responseCt = 0
    @volatile var closeReceived = false
    val s = new ServerBase(port) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: UpdateAllProgressCommand =>
                progressReceivedCt += 1
                assert(p.progressMarks.keys.max === progressReceivedCt)
                assert(p.progressMarks.values.sum === 25)
                ctx.writeAndFlush(new ProgressUpdateCommand(0, 0, progressReceivedCt, 0, false))
              case c: CloseCommand =>
                // shut down the server
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceived = true
            }
          }
        }
      }
    }
    s.run()
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, hosts, 0L, 100L)
    val c = new ClientGroupBase(rc) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: ProgressUpdateCommand =>
                sumResponseCt += p.numProcessed
                responseCt += 1
            }
          }
        }
      }
    }
    val localHostConfiguration = new HostConfiguration(localhost, port)
    c.connect(Array(localHostConfiguration))
    for (i <- 1 to n) {
      c.issueCommand(0, new UpdateAllProgressCommand(m + (i -> 10)), ImmediateEventExecutor.INSTANCE)
    }
    c.issueCommand(0, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    logger.debug("close issued")
    s.close()
    assert(progressReceivedCt === n)
    assert(responseCt === n)
    assert(sumResponseCt === n * (n + 1) / 2)
    assert(closeReceived)
  }

  "A client and two servers should" should "ping back and forth" in {
    logger.debug("starting two server ping test")
    val localhost = "127.0.0.1"
    val port1 = 15567
    val port2 = 15568
    val hosts = Array(new HostConfiguration(localhost, port1), new HostConfiguration(localhost, port2))
    val m = Map[Int,Long](0 -> 15L)
    val n = 1000
    val progressReceivedCt = new AtomicInteger()
    val sumResponseCt = new AtomicLong()
    val responseCt = new AtomicInteger()
    val closeReceivedCt = new AtomicInteger()
    def getServer(port: Int): ServerBase = new ServerBase(port) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case c: CloseCommand =>
                // shut down the server
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceivedCt.incrementAndGet()
              case p: UpdateAllProgressCommand =>
                val x = progressReceivedCt.incrementAndGet()
                ctx.writeAndFlush(new ProgressUpdateCommand(0, 0, x, 0, false))
            }
          }
        }
      }
    }
    val s1 = getServer(port1)
    s1.run()
    val s2 = getServer(port2)
    s2.run()
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, hosts, 0L, 100L)
    val c = new ClientGroupBase(rc) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: ProgressUpdateCommand =>
                sumResponseCt.addAndGet(p.numProcessed)
                responseCt.addAndGet(1)
            }
          }
        }
      }
    }
    val localHostConfiguration1 = new HostConfiguration(localhost, port1)
    val localHostConfiguration2 = new HostConfiguration(localhost, port2)
    c.connect(Array(localHostConfiguration1, localHostConfiguration2))
    for (i <- 1 to n) {
      c.issueCommand(i % 2, new UpdateAllProgressCommand(m + (i -> 10)), ImmediateEventExecutor.INSTANCE)
    }
    c.issueCommand(0, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    c.issueCommand(1, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    logger.debug("close issued")
    s1.close()
    s2.close()
    assert(progressReceivedCt.get() === n)
    assert(responseCt.get() === n)
    assert(sumResponseCt.get() === n * (n + 1) / 2)
    assert(closeReceivedCt.get() === 2)
  }


  "A client and many servers should" should "ping back and forth" in {
    logger.debug("starting many server ping test")
    val localhost = "127.0.0.1"
    val numServers = 50
    val numMessages = 10000
    val ports = (0 until numServers).map(_ + 15567).toArray
    val hosts = ports.map(new HostConfiguration(localhost, _)).toArray
    val m = Map[Int,Long](0 -> 15L)
    val progressReceivedCt = new AtomicInteger()
    val sumResponseCt = new AtomicLong()
    val responseCt = new AtomicInteger()
    val closeReceivedCt = new AtomicInteger()
    val servers = ports.map(port => new ServerBase(port) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case c: CloseCommand =>
                // shut down the server
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceivedCt.incrementAndGet()
              case p: UpdateAllProgressCommand =>
                val x = progressReceivedCt.incrementAndGet()
                ctx.writeAndFlush(new ProgressUpdateCommand(0, 0, x, 0, false))
            }
          }
        }
      }
    })
    servers.foreach(_.run())
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, hosts, 0L, 100L)
    val c = new ClientGroupBase(rc) {
      override def getHandler(): ChannelHandler = {
        new ChannelHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            val c = msg.asInstanceOf[Command]
            c match {
              case p: ProgressUpdateCommand =>
                sumResponseCt.addAndGet(p.numProcessed)
                responseCt.addAndGet(1)
            }
          }
        }
      }
    }
    c.connect(ports.map(port => new HostConfiguration(localhost, port)))
    for (i <- 1 to numMessages) {
      c.issueCommand(i % numServers, new UpdateAllProgressCommand(m + (i -> 10)), ImmediateEventExecutor.INSTANCE)
    }
    for (i <- 0 until numServers) {
      c.issueCommand(i, new CloseCommand(), ImmediateEventExecutor.INSTANCE)
    }
    logger.debug("close issued")
    servers.foreach(_.close)
    assert(progressReceivedCt.get() === numMessages)
    assert(responseCt.get() === numMessages)
    assert(sumResponseCt.get === numMessages * (numMessages + 1) / 2)
    assert(closeReceivedCt.get() === numServers)
  }
}
