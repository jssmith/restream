package replaydb.exec.spam

import java.io.FileInputStream

import replaydb.exec.spam.DistributedSpamDetector._
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev.{HasRuntimeInterface, PrintSpamCounter}
import replaydb.runtimedev.serialImpl.ReplayStateFactory
import replaydb.util.ProgressMeter

/**
 * Single-threaded implementation of spam detector
 */
object SerialSpamDetector extends App {

  if (args.length != 2) {
    println(
      """Usage: SerialSpamDetector spamDetector filename
        |  Example values:
        |    spamDetector   = replaydb.exec.spam.SpamDetectorStats
        |    filename       = ~/data/events-split-1/events.out-0
      """.stripMargin)
    System.exit(1)
  }

  val spamDetector = Class.forName(args(0)).asInstanceOf[Class[HasRuntimeInterface with HasSpamCounter]]
  val inputFilename = args(1)
  val eventStorage = new SocialNetworkStorage
  val stats = spamDetector
    .getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
    .newInstance(new ReplayStateFactory)
  val si = stats.getRuntimeInterface
  var lastTimestamp = 0L
  val pm = new ProgressMeter(printInterval = 1000000, () => { si.updateAllPhases(new PrintSpamCounter(lastTimestamp)); ""})
  val r = eventStorage.readEvents(new FileInputStream(inputFilename), e => {
    si.updateAllPhases(e)
    lastTimestamp = e.ts
    pm.increment()
  })
  pm.finished()
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue))
}
