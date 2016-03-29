package replaydb.exec.spam

import org.scalatest.FlatSpec

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-1/events.out 1
 *
 */
class SerialWordCountSpamDetectorSpec extends FlatSpec with TestRunner {

  "A WordCountDetector" should "run with binding API" in {
    runSerialSpamDetector(classOf[WordCountStats], DataDesc.SHORT)
  }

  it should "run with direct API" in {
    runSerialSpamDetector(classOf[WordCountStats2], DataDesc.SHORT)
  }

}
