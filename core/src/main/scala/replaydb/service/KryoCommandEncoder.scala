package replaydb.service

import com.esotericsoftware.kryo.io.Output
import com.typesafe.scalalogging.Logger
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.slf4j.LoggerFactory
import replaydb.service.driver.{Command, KryoCommands}

class KryoCommandEncoder extends OneToOneEncoder with KryoCommands {
  val logger = Logger(LoggerFactory.getLogger(classOf[KryoCommandEncoder]))
  val output = new Output(KryoCommands.MAX_KRYO_MESSAGE_SIZE + 4)
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    output.clear()
    // TODO is there any way to do this without having to copy the buffer, instead writing directly?
    kryo.writeClassAndObject(output, msg)
    val len = output.position()
    val buf = ChannelBuffers.buffer(len + 4)
    buf.writeInt(len)
    logger.debug(s"kryo encoding class of type ${msg.getClass} of size $len")
    buf.writeBytes(output.getBuffer, 0, len)
    buf
  }
}