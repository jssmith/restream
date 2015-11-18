package replaydb.runtimedev.threadedImpl

import java.io.FileInputStream

import replaydb.event.Event
import replaydb.io.SocialNetworkStorage

class MultiReaderEventSource(fn: String, numReaders: Int, bufferSize: Int) extends Thread(s"read-$fn") {
  private val buffer = new Array[Event](bufferSize)
  private val positions = new Array[Long](numReaders)

  private var pos = 0L
  private var done = false
  private var numReadersRegistered = 0

  private def nextReaderNum(): Int = {
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
    val updateDelta = 100
    var localPos = pos
    var posUpdate = localPos + updateDelta

    var limit = positions.min + bufferSize
    eventStorage.readEvents(new FileInputStream(fn), e => {
      if (localPos >= limit) {
        positions.synchronized {
          while ({ limit = positions.min + bufferSize; localPos >= limit }) {
//            println(s"main thread blocked pos=$pos limit=$limit")
            positions.wait()
          }
        }
      }
      buffer((localPos % bufferSize).toInt) = e
      localPos += 1
      if (localPos == posUpdate) {
        this.synchronized {
          pos = localPos
          this.notifyAll()
        }
        posUpdate += updateDelta
      }
    })
    this.synchronized {
      pos = localPos
      done = true
      this.notifyAll()
    }
  }

  def readEvents(f: (Event, AnyRef) => Unit): Unit = {
    val readerId = nextReaderNum()
    val readerUpdateDelta = 100
    var readerPos = 0L
    var readerPosUpdate = readerPos + readerUpdateDelta
    var localDone = false
    var posLimit = this.synchronized { pos }
    do {
      while (readerPos < posLimit) {
        f(buffer((readerPos % bufferSize).toInt), positions)
        readerPos += 1
        if (readerPos == readerPosUpdate) {
          positions.synchronized {
            positions(readerId) = readerPos
//            println("notify on positions")
            positions.notifyAll()
          }
          readerPosUpdate += readerUpdateDelta
        }
      }
      this.synchronized {
        while ( { posLimit = pos; localDone = done; readerPos >= posLimit && !localDone}) {
//          println(s"reader $readerId blocked at readerPos=$readerPos posLimit=$posLimit done=$localDone")
          this.wait()
        }
      }
    } while (readerPos < posLimit || !localDone)
  }
}
