package replaydb.service

import com.typesafe.scalalogging.Logger
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInboundHandler}
import org.scalatest.FlatSpec
import org.slf4j.LoggerFactory
import replaydb.service.driver.Hosts.HostConfiguration
import replaydb.service.driver.{Command, CloseCommand, UpdateAllProgressCommand, RunConfiguration}

class ClientServerSpec extends FlatSpec {
  val logger = Logger(LoggerFactory.getLogger(classOf[ClientServerSpec]))

  "A single client and sever" should "exchange messages" in {
    logger.debug("starting client server tests")
    val localhost = "127.0.0.1"
    val port = 15567
    val m = Map[Int,Long](1->1L)
    @volatile var progressReceived = false
    @volatile var closeReceived = false
    val printingHandlerFactory = new ServiceHandlerFactory {
      override def getHandler(): ChannelInboundHandler = {
        new ChannelInboundHandlerAdapter() {
          override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
            println(s"received $msg")
            // shut down the server
            val c = msg.asInstanceOf[Command]
            c match {
              case c: CloseCommand =>
                ctx.channel().close().sync()
                ctx.channel().parent().close().sync()
                closeReceived = true
              case p: UpdateAllProgressCommand =>
                assert(p.progressMarks === m)
                progressReceived = true
            }
          }
        }
      }
    }
    val s = new ServerBase(port, printingHandlerFactory)
    s.run()
    logger.debug("server started")
    val rc = new RunConfiguration(1, 2, 0L, 100L)
    val c = new ClientGroupBase(rc, printingHandlerFactory)
    val localHostConfiguration = new HostConfiguration(localhost, port)
    c.connect(Array(localHostConfiguration))
    c.issueCommand(0, new UpdateAllProgressCommand(m))
    c.issueCommand(0, new CloseCommand())
    logger.debug("close issued")
    s.close()
    assert(progressReceived)
    assert(closeReceived)
  }
}
