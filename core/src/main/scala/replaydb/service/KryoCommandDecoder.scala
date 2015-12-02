package replaydb.service

import java.util

import com.esotericsoftware.kryo.io.Input
import com.typesafe.scalalogging.Logger
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import org.slf4j.LoggerFactory
import replaydb.service.driver.KryoCommands

class KryoCommandDecoder extends ByteToMessageDecoder with KryoCommands {
  val logger = Logger(LoggerFactory.getLogger(classOf[KryoCommandDecoder]))
   val buf = new Array[Byte](KryoCommands.MAX_KRYO_MESSAGE_SIZE)
   val input = new Input(buf)

   override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
     val len = in.readableBytes()
     if (len > 0) {
       if (len > buf.length) {
         throw new RuntimeException("Buffer size exceeded")
       }
       input.rewind()
       // TODO any way to do this without copying the buffer?
       in.readBytes(buf, 0, len)
       input.setLimit(len)
       val obj = kryo.readClassAndObject(input)
       out.add(obj)
       logger.debug(s"kryo decoded class of type ${obj.getClass}")
     }
   }
 }

