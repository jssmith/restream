package replaydb.service

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.slf4j.LoggerFactory
import replaydb.service.driver.KryoCommands

class KryoCommandDecoder extends FrameDecoder with KryoCommands {
  val logger = Logger(LoggerFactory.getLogger(classOf[KryoCommandDecoder]))
   val buf = new Array[Byte](KryoCommands.MAX_KRYO_MESSAGE_SIZE + 4)
   val input = new Input(buf)

   override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
     if (buffer.readableBytes() < 4) {
       return null
     }
     buffer.markReaderIndex()
     val dataLen = buffer.readInt()
     if (dataLen > buf.length) {
       throw new RuntimeException("Buffer size exceeded")
     }
     if (buffer.readableBytes() < dataLen) {
       buffer.resetReaderIndex()
       return null
     }
     input.rewind()
     // TODO any way to do this without copying the buffer?
     buffer.readBytes(buf, 0, dataLen)
     input.setLimit(dataLen)
     val obj = kryo.readClassAndObject(input)
     logger.debug(s"kryo decoded class of type ${obj.getClass}")
     obj
   }
 }

