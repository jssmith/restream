package replaydb.runtimedev.threadedImpl

import java.io.FileInputStream

import replaydb.event.Event
import replaydb.io.SocialNetworkStorage

class MultiReaderEventSource(fn: String, numReaders: Int, bufferSize: Int) extends Thread {
  val buffer = new Array[Event](bufferSize)
  val positions = new Array[Int](numReaders)

  var pos = 0L
  var done = false
  var numReadersRegistered = 0

  def nextReaderNum(): Int = {
    if (numReadersRegistered < numReaders) {
      val ret = numReadersRegistered
      numReadersRegistered += 1
      ret
    } else {
      throw new RuntimeException("too many readers registered")
    }
  }

  override def run(): Unit = {
    val eventStorage = new SocialNetworkStorage

    var limit = positions.min + bufferSize
    eventStorage.readEvents(new FileInputStream(fn), e => {
      if (pos >= limit) {
        positions.synchronized {
          while ({ limit = positions.min + bufferSize; pos >= limit }) {
//            println(s"main thread blocked pos=$pos limit=$limit")
            positions.wait()
          }
        }
      }
      buffer((pos % bufferSize).toInt) = e
      // TODO this synchronization could be expensive
      this.synchronized {
        pos = pos + 1
        this.notifyAll()
      }
    })
    this.synchronized {
      done = true
      this.notifyAll()
    }
  }

  def readEvents(f: Event => Unit): Unit = {
    val readerId = nextReaderNum()
    var readerPos = 0
    var localDone = false
    var posLimit = this.synchronized { pos }
    do {
      while (readerPos < posLimit) {
        f(buffer(readerPos % bufferSize))
        readerPos += 1
        positions.synchronized {
          positions(readerId) = readerPos
//          println("notify on positions")
          positions.notifyAll()
        }
      }
      this.synchronized {
        while ( { posLimit = pos; localDone = done; readerPos >= posLimit && !localDone}) {
//          println(s"reader $readerId blocked at readerPos=$readerPos posLimit=$posLimit done=$localDone")
          this.wait()
        }
      }
    } while (!localDone)
  }
}
