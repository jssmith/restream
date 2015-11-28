package replaydb.service

import com.esotericsoftware.kryo.io.Output
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder
import replaydb.service.driver.{Command, KryoCommands}

class KryoCommandEncoder extends MessageToByteEncoder[Command] with KryoCommands {
  val output = new Output(KryoCommands.MAX_KRYO_MESSAGE_SIZE + 4)
  override def encode(ctx: ChannelHandlerContext, msg: Command, out: ByteBuf): Unit = {
    output.clear()
    kryo.writeClassAndObject(output, msg)
    val len = output.position()
    out.writeInt(len)
    out.writeBytes(output.getBuffer, 0, len)
  }
}
