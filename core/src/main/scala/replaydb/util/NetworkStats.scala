package replaydb.util

import java.util.concurrent.atomic.AtomicLong

class NetworkStats {

  val sent = new AtomicLong
  val received = new AtomicLong

  def reset(): Unit = {
    sent.set(0)
    received.set(0)
  }

  def recordSent(bytes: Long): Unit = {
    sent.addAndGet(bytes)
  }

  def recordReceived(bytes: Long): Unit = {
    received.addAndGet(bytes)
  }

  override def toString(): String = {
    s"{sent=$sent,received=$received}"
  }

}
