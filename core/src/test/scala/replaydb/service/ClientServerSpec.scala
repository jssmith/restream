package replaydb.service

import java.util.concurrent.{TimeUnit, CountDownLatch}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.typesafe.scalalogging.Logger
import org.jboss.netty.channel.{ChannelHandlerContext, ChannelUpstreamHandler, MessageEvent, SimpleChannelUpstreamHandler}
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
    val progressReceived = new AtomicBoolean(false)
    val closeReceived = new CountDownLatch(1)
    val s = new ServerBase(port) {
      override def getHandler(): ChannelUpstreamHandler = {
        new SimpleChannelUpstreamHandler() {
          override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
            val c = me.getMessage.asInstanceOf[Command]
            c match {
              case p: UpdateAllProgressCommand =>
                assert(p.progressMarks === m)
              case c: CloseCommand =>
                // shut down the server
                ctx.getChannel.close()
                ctx.getChannel.getParent.close()
                closeReceived.countDown()
                progressReceived.set(true)
            }
          }
        }
      }
    }
    try {
      s.run()
      logger.debug("server started")
      val rc = new RunConfiguration(1, 2, hosts, 0L, 100L, 100000)
      val c = new ClientGroupBase(rc) {
        override def getHandler(): ChannelUpstreamHandler= {
          new SimpleChannelUpstreamHandler() {
            override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = ???
          }
        }
      }
      val localHostConfiguration = new HostConfiguration(localhost, port)
      c.connect(Array(localHostConfiguration))
      c.issueCommand(0, new UpdateAllProgressCommand(m))
      c.issueCommand(0, new CloseCommand())
      logger.debug("close issued")
      assert(closeReceived.await(1, TimeUnit.SECONDS))
      assert(progressReceived.get())
    } finally {
      s.close()
    }
  }

  they should "exchange multiple messages" in {
    logger.debug("starting multiple messages test")
    val localhost = "127.0.0.1"
    val port = 15567
    val hosts = Array(new HostConfiguration(localhost, port))
    val m = Map[Int,Long](0 -> 15L)
    val n = 1000
    val progressReceivedCt = new AtomicInteger()
    val closeReceived = new CountDownLatch(1)
    val s = new ServerBase(port) {
      override def getHandler(): ChannelUpstreamHandler= {
        new SimpleChannelUpstreamHandler() {
          override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
            try {
              val c = me.getMessage.asInstanceOf[Command]
              c match {
                case p: UpdateAllProgressCommand =>
                  val latestProgressCt = progressReceivedCt.incrementAndGet()
                  assert(p.progressMarks.keys.max === latestProgressCt)
                  assert(p.progressMarks.values.sum === 25)
                case c: CloseCommand =>
                  // shut down the server
                  me.getChannel.close()
                  me.getChannel.getParent.close()
                  closeReceived.countDown()
              }
            } catch {
              case e: Throwable =>
                logger.error("problem in messageReceived", e)
                throw e
            }
          }
        }
      }
    }
    try {
      s.run()
      logger.debug("server started")
      val rc = new RunConfiguration(1, 2, hosts, 0L, 100L, 100000)
      val c = new ClientGroupBase(rc) {
        override def getHandler(): ChannelUpstreamHandler= {
          new SimpleChannelUpstreamHandler() {
            override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = ???
          }
        }
      }
      val localHostConfiguration = new HostConfiguration(localhost, port)
      c.connect(Array(localHostConfiguration))
      for (i <- 1 to n) {
        c.issueCommand(0, new UpdateAllProgressCommand(m + (i -> 10)))
      }
      c.issueCommand(0, new CloseCommand())
      logger.debug("close issued")
      assert(closeReceived.await(1, TimeUnit.SECONDS))
      assert(progressReceivedCt.get() === n)
    } finally {
      s.close()
    }
  }

  they should "ping back and forth" in {
    logger.debug("starting ping test")
    val localhost = "127.0.0.1"
    val port = 15567
    val hosts = Array(new HostConfiguration(localhost, port))
    val m = Map[Int,Long](0 -> 15L)
    val n = 10
    val progressReceivedCt = new AtomicInteger()
    val sumResponseCt = new AtomicInteger()
    val responseCt = new CountDownLatch(n)
    val closeReceived = new CountDownLatch(1)
    val s = new ServerBase(port) {
      override def getHandler(): ChannelUpstreamHandler= {
        new SimpleChannelUpstreamHandler() {
          override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
            try {
              val c = me.getMessage.asInstanceOf[Command]
              c match {
                case p: UpdateAllProgressCommand =>
                  val latestProgressCt = progressReceivedCt.incrementAndGet()
                  assert(p.progressMarks.keys.max === latestProgressCt)
                  assert(p.progressMarks.values.sum === 25)
                  me.getChannel.write(new ProgressUpdateCommand(0, 0, latestProgressCt, 0, false))
                case c: CloseCommand =>
                  // shut down the server
                  me.getChannel.close()
                  me.getChannel.getParent.close()
                  closeReceived.countDown()
              }
            } catch {
              case e: Throwable =>
                logger.error("problem in messageReceived", e)
                throw e
            }
          }
        }
      }
    }
    try {
      s.run()
      logger.debug("server started")
      val rc = new RunConfiguration(1, 2, hosts, 0L, 100L, 100000)
      val c = new ClientGroupBase(rc) {
        override def getHandler(): ChannelUpstreamHandler= {
          new SimpleChannelUpstreamHandler() {
            override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
              val c = me.getMessage.asInstanceOf[Command]
              c match {
                case p: ProgressUpdateCommand =>
                  sumResponseCt.addAndGet(p.numProcessed)
                  responseCt.countDown()
              }
            }
          }
        }
      }
      val localHostConfiguration = new HostConfiguration(localhost, port)
      c.connect(Array(localHostConfiguration))
      for (i <- 1 to n) {
        c.issueCommand(0, new UpdateAllProgressCommand(m + (i -> 10)))
      }
      assert(responseCt.await(5, TimeUnit.SECONDS))
      c.issueCommand(0, new CloseCommand())
      assert(closeReceived.await(1, TimeUnit.SECONDS))
      assert(progressReceivedCt.get() === n)
      assert(sumResponseCt.get() === n * (n + 1) / 2)
    } finally {
      s.close()
    }
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
    val sumResponseCt = new AtomicInteger()
    val responseCt = new CountDownLatch(n)
    val closeReceivedCt = new CountDownLatch(2)
    def getServer(port: Int): ServerBase = new ServerBase(port) {
      override def getHandler(): ChannelUpstreamHandler= {
        new SimpleChannelUpstreamHandler() {
          override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
            val c = me.getMessage.asInstanceOf[Command]
            c match {
              case c: CloseCommand =>
                // shut down the server
                me.getChannel.close()
                me.getChannel.getParent.close()
                closeReceivedCt.countDown()
              case p: UpdateAllProgressCommand =>
                val x = progressReceivedCt.incrementAndGet()
                me.getChannel.write(new ProgressUpdateCommand(0, 0, x, 0, false))
            }
          }
        }
      }
    }
    val s1 = getServer(port1)
    val s2 = getServer(port2)
    try {
      s1.run()
      s2.run()
      logger.debug("servers started")
      val rc = new RunConfiguration(1, 2, hosts, 0L, 100L, 100000)
      val c = new ClientGroupBase(rc) {
        override def getHandler(): ChannelUpstreamHandler= {
          new SimpleChannelUpstreamHandler() {
            override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
              val c = me.getMessage.asInstanceOf[Command]
              c match {
                case p: ProgressUpdateCommand =>
                  sumResponseCt.addAndGet(p.numProcessed)
                  responseCt.countDown()
              }
            }
          }
        }
      }
      val localHostConfiguration1 = new HostConfiguration(localhost, port1)
      val localHostConfiguration2 = new HostConfiguration(localhost, port2)
      c.connect(Array(localHostConfiguration1, localHostConfiguration2))
      for (i <- 1 to n) {
        c.issueCommand(i % 2, new UpdateAllProgressCommand(m + (i -> 10)))
      }
      assert(responseCt.await(5, TimeUnit.SECONDS))
      c.issueCommand(0, new CloseCommand())
      c.issueCommand(1, new CloseCommand())
      assert(closeReceivedCt.await(1, TimeUnit.SECONDS))
      assert(progressReceivedCt.get() === n)
      assert(sumResponseCt.get() === n * (n + 1) / 2)
    } finally {
      s1.close()
      s2.close()
    }
  }


  "A client and many servers" should "ping back and forth" in {
    logger.debug("starting many server ping test")
    val localhost = "127.0.0.1"
    val numServers = 25
    val numMessages = 10000
    val ports = (0 until numServers).map(_ + 15567).toArray
    val hosts = ports.map(new HostConfiguration(localhost, _)).toArray
    val m = Map[Int,Long](0 -> 15L)
    val progressReceivedCt = new AtomicInteger()
    val sumResponseCt = new AtomicInteger()
    val responseCt = new CountDownLatch(numMessages)
    val closeReceivedCt = new CountDownLatch(numServers)
    val servers = ports.map(port => new ServerBase(port) {
      override def getHandler(): ChannelUpstreamHandler= {
        new SimpleChannelUpstreamHandler() {
          override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
            val c = me.getMessage.asInstanceOf[Command]
            c match {
              case c: CloseCommand =>
                // shut down the server
                Thread.sleep(100)
                me.getChannel.close()
                me.getChannel.getParent.close()
                closeReceivedCt.countDown()
              case p: UpdateAllProgressCommand =>
                val x = progressReceivedCt.incrementAndGet()
                me.getChannel.write(new ProgressUpdateCommand(0, 0, x, 0, false))
            }
          }
        }
      }
    })
    try {
      servers.foreach(_.run())
      logger.debug("server started")
      val rc = new RunConfiguration(1, 2, hosts, 0L, 100L, 100000)
      val c = new ClientGroupBase(rc) {
        override def getHandler(): ChannelUpstreamHandler= {
          new SimpleChannelUpstreamHandler() {
            override def messageReceived(ctx: ChannelHandlerContext, me: MessageEvent): Unit = {
              val c = me.getMessage.asInstanceOf[Command]
              c match {
                case p: ProgressUpdateCommand =>
                  sumResponseCt.addAndGet(p.numProcessed)
                  responseCt.countDown()
              }
            }
          }
        }
      }
      c.connect(ports.map(port => new HostConfiguration(localhost, port)))
      for (i <- 1 to numMessages) {
        c.issueCommand(i % numServers, new UpdateAllProgressCommand(m + (i -> 10)))
      }
      assert(responseCt.await(5, TimeUnit.SECONDS))
      for (i <- 0 until numServers) {
        c.issueCommand(i, new CloseCommand())
      }
      assert(closeReceivedCt.await(5, TimeUnit.SECONDS))
      assert(progressReceivedCt.get() === numMessages)
      assert(sumResponseCt.get === numMessages * (numMessages + 1) / 2)
    } finally {
      servers.foreach(_.close)
    }
  }
}
