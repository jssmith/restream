package replaydb.service

import com.esotericsoftware.kryo.io.Output
import com.typesafe.scalalogging.Logger
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import org.slf4j.LoggerFactory
import replaydb.service.driver.{Command, KryoCommands}

class KryoCommandEncoder extends MessageToByteEncoder[Command] with KryoCommands {
  val logger = Logger(LoggerFactory.getLogger(classOf[KryoCommandEncoder]))
  val output = new Output(KryoCommands.MAX_KRYO_MESSAGE_SIZE + 4)
  override def encode(ctx: ChannelHandlerContext, msg: Command, out: ByteBuf): Unit = {
    logger.debug(s"kryo encoding class of type ${msg.getClass}")
    output.clear()
    // TODO is there any way to do this without having to copy the buffer, instead writing directly?
    kryo.writeClassAndObject(output, msg)
    val len = output.position()
    out.writeInt(len)
    out.writeBytes(output.getBuffer, 0, len)
  }
}
