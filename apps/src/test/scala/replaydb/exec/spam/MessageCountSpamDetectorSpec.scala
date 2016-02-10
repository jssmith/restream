package replaydb.exec.spam

import org.scalatest.FlatSpec

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-2/events.out 2
 *
 */
class MessageCountSpamDetectorSpec extends FlatSpec with TestRunner {

  it should "run with a two-way split" in {
    runDistributedSpamDetector(2, classOf[MessageCountSpamDetectorStats], DataDesc.SHORT)
  }

}
