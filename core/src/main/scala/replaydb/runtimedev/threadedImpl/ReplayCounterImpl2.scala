package replaydb.runtimedev.threadedImpl

import java.util.PriorityQueue

import replaydb.runtimedev.ReplayCounter

import scala.collection.mutable.ArrayBuffer


class ReplayCounterImpl2 extends ReplayCounter {
  case class CounterRecord(ts: Long, value: Long) extends Ordered[CounterRecord] {
    override def compare(that: CounterRecord): Int = {
      ts.compareTo(that.ts)
    }
  }
  val updates = new PriorityQueue[CounterRecord]()
  var history: ArrayBuffer[CounterRecord] = null
  var lastRead = Long.MinValue
  override def add(value: Long, ts: Long): Unit = {
    if (ts <= lastRead) {
      throw new IllegalArgumentException(s"add at $ts must follow get at $lastRead")
    }
    updates.add(new CounterRecord(ts = ts, value = value))
  }

  override def get(ts: Long): Long = {
    lastRead = math.max(lastRead, ts)
    // TODO avoid creating this if nothing needed
    val toSort = ArrayBuffer[CounterRecord]()
    while (updates.size() > 0 && updates.peek().ts <= ts) {
      toSort += updates.poll()
    }
    if (!toSort.isEmpty) {
      var cumSum: Long = if (history != null && history.nonEmpty) {
        history(history.size - 1).value
      } else {
        0
      }
      val sortedUpdates = toSort.sorted.map(cr => {
        cumSum += cr.value
//        println(cr.ts + " " + cumSum)
        new CounterRecord(ts = cr.ts, value = cumSum)
      })
      if (history != null) {
        history ++= sortedUpdates
      } else {
        history = new ArrayBuffer[CounterRecord] ++ sortedUpdates
      }
    }
//    println(s"get at timestamp ${ts}")
//    println(history + " || " + { val a = updates.toArray; util.Arrays.sort(a); a.take(5).mkString(",")})
    if (history != null) {
      for (cr <- history.reverseIterator) {
        if (cr.ts <= ts) {
//          println(s"returning ${cr.value}")
          return cr.value
        }
      }
    }
//    println("not found - return 0")
    0
  }
}
