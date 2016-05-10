package replaydb

import scala.collection.mutable.ArrayBuffer

object TSValList {
  def apply[A](items: TSVal[A]*): TSValList[A] = {
    val tsvl = new TSValList[A](items.length)
    tsvl.buf ++= items
    tsvl
  }
}

class TSValList[A](initialSize: Int = 0) {

  val buf = new ArrayBuffer[TSVal[A]](initialSize)

  // Combine takes a total and an incremental and returns a new total
  // Assumes that the lists are currently in sorted order (they should never not be)
  def merge[B](incremental: TSValList[B], combine: (A, B) => A): TSValList[A] = {
    if (buf.last._1 > incremental.buf.head._1) {
      throw new IllegalArgumentException("All of the incremental list TS must be after the current list!")
    }
    var runningTotal = buf.last._2
    for (inc <- incremental.buf) {
      runningTotal = combine(runningTotal, inc._2)
      buf.append(inc._1 -> runningTotal)
    }
    this
  }

  // Consider *this* to be an incremental list. Start with zeroVal, and combine
  // one-at-a-time the value into the running total. Length of return is same as
  // length of input
  def sumOverIncremental[B](zeroVal: B, combine: (B, A) => B): TSValList[B] = {
    var runningTotal = zeroVal
    val newList = new TSValList[B](buf.size)
    for ((ts, value) <- buf) {
      runningTotal = combine(runningTotal, value)
      newList.buf.append(ts -> runningTotal)
    }
    newList
  }

  //
  def evaluateAgainst[B, C](state: Option[TSValList[B]], evaluate: (A, Option[B]) => C): TSValList[C] = {
    var currentState: Option[B] = None
    val newList = new TSValList[C](buf.size)
    val nextStates = if (state.isEmpty) ArrayBuffer() else state.get.buf
    var stateIndex = 0
    for ((ts, value) <- buf) {
      while (stateIndex < nextStates.length && nextStates(stateIndex)._1 <= ts) {
        // next state is applicable to us
        currentState = Some(nextStates(stateIndex)._2)
        stateIndex += 1
      }
      newList.buf.append(ts -> evaluate(value, currentState))
    }
    newList
  }

  def gcUpTo(gcTs: Long): TSValList[A] = {
    var removeCount = -1
    buf.forall { case (ts, _) =>
      removeCount += 1
      ts < gcTs
    }
    buf.remove(0, removeCount)
    this
  }
}
