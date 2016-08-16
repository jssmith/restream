package replaydb.exec.spam

import org.apache.log4j.{Logger, PatternLayout, FileAppender, Level}
import replaydb.event.{NewFriendshipEvent, MessageEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.runtimedev.causalLoggingImpl.{Logger=>li,ReplayStateFactory}
import replaydb.runtimedev.threadedImpl.MultiReaderEventSource
import replaydb.runtimedev.{HasRuntimeInterface, PrintSpamCounter}
import replaydb.util.ProgressMeter

/**
 * Logs causal dependencies
 * Uses a separate thread to deserialize saved event history, increasing performance.
 */
object CausalLoggingSerialSpamDetector extends App {

  if (args.length != 3) {
    println(
      """Usage: LoggingSerialSpamDetector spamDetector filename outputfilename
        |  Example values:
        |    spamDetector   = replaydb.exec.spam.SpamDetectorStats
        |    filename       = ~/data/events-split-1/events.out-0
      """.stripMargin)
    System.exit(1)
  }

  val spamDetector = Class.forName(args(0)).asInstanceOf[Class[HasRuntimeInterface with HasSpamCounter]]
  val inputFilename = args(1)
  val outputFilename = args(2)

  // Create the logger
  val a = new FileAppender()
//  a.setName("FileLogger")
  a.setFile(outputFilename)
  a.setLayout(new PatternLayout("%m%n"))
  a.setThreshold(Level.INFO)
  a.setAppend(false)
  a.activateOptions()
  Logger.getLogger(classOf[li]).addAppender(a)

  val eventStorage = new SocialNetworkStorage
  val stats = spamDetector
    .getConstructor(classOf[replaydb.runtimedev.ReplayStateFactory])
    .newInstance(new ReplayStateFactory)
  val si = stats.getRuntimeInterface
  var lastTimestamp = 0L
  var timestampAdjustment = 0L
  val pm = new ProgressMeter(printInterval = 1000000, () => { si.updateAllPhases(new PrintSpamCounter(lastTimestamp)); ""})
  val es = new MultiReaderEventSource(inputFilename, 1, 100000)
  es.start()
  es.readEvents(e => {
    if (e.ts == lastTimestamp) {
      timestampAdjustment += 1
    }
    si.updateAllPhases(e match {
      case me: MessageEvent =>
        MessageEvent(
          me.ts + timestampAdjustment,
          messageId = me.messageId,
          senderUserId = me.senderUserId,
          recipientUserId = me.recipientUserId,
          senderIp = me.senderIp,
          content = me.content
        )
      case nfe: NewFriendshipEvent =>
        NewFriendshipEvent(
          nfe.ts + timestampAdjustment,
          userIdA = nfe.userIdA,
          userIdB =  nfe.userIdB
        )
    })
    lastTimestamp = e.ts
    pm.increment()
  })
  si.updateAllPhases(PrintSpamCounter(lastTimestamp))
  pm.finished()
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue))
}
