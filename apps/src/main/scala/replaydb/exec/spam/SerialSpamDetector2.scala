package replaydb.exec.spam

import replaydb.runtimedev.serialImpl.ReplayStateFactory
import replaydb.runtimedev.threadedImpl.MultiReaderEventSource
import replaydb.util.ProgressMeter

/**
 * Uses a separate thread to deserialize saved event history, increasing performance.
 */
object SerialSpamDetector2 extends App {

  if (args.length != 1) {
    println("Usage: SerialSpamDetector2 filename")
    System.exit(1)
  }

  val inputFilename = args(0)
  val stats = new SpamDetectorStats(new ReplayStateFactory)
  val si = stats.getRuntimeInterface
  var lastTimestamp = 0L
  val pm = new ProgressMeter(printInterval = 1000000, () => { si.updateAllPhases(new PrintSpamCounter(lastTimestamp)); ""})
  val es = new MultiReaderEventSource(inputFilename, 1, 100000)
  es.start()
  es.readEvents(e => {
    si.updateAllPhases(e)
    lastTimestamp = e.ts
    pm.increment()
  })
  pm.finished()
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue))
}
