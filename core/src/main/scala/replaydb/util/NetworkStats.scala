package replaydb.util

import java.util.concurrent.atomic.AtomicLong

class NetworkStats {

  val sentBytes = new AtomicLong()
  val receivedBytes = new AtomicLong()
  val sentObjects = new AtomicLong()
  val receivedObjects = new AtomicLong()
  val sentKryoTime = new AtomicLong()
  val receivedKryoTime = new AtomicLong()

  def reset(): Unit = {
    sentBytes.set(0)
    receivedBytes.set(0)
  }

  def recordSent(bytes: Long, kryoTimeNanos: Long): Unit = {
    sentBytes.addAndGet(bytes)
    sentObjects.incrementAndGet()
    sentKryoTime.addAndGet(kryoTimeNanos)
  }

  def recordReceived(bytes: Long, kryoTimeNanos: Long): Unit = {
    receivedBytes.addAndGet(bytes)
    receivedObjects.incrementAndGet()
    receivedKryoTime.addAndGet(kryoTimeNanos)
  }

  override def toString(): String = {
    s"{sent_bytes=$sentBytes,sent_objects=$sentObjects,sent_kryo_ms=${sentKryoTime.get()/1000000}" +
      s",recv_bytes=$receivedBytes,recv_objects=$receivedObjects,recv_kryo_ms=${receivedKryoTime.get()/1000000}}"
  }

}
