package replaydb.exec.spam

import org.scalatest.FlatSpec

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-2/events.out 2
 *
 */
class DistributedWordCountSpamDetectorSpec extends FlatSpec with TestRunner {

  "A Distributed Word Count Spam Detector" should "run with a two-way split and binding API" in {
    runDistributedSpamDetector(2, classOf[WordCountStats], DataDesc.SHORT)
  }

  it should "run with a two-way split and direct API" in {
    runDistributedSpamDetector(2, classOf[WordCountStats2], DataDesc.SHORT)
  }

}
