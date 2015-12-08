package replaydb.exec.spam

import org.scalatest.FlatSpec

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-1/events.out 1
 *
 */
class SerialSpamDetectorSpec extends FlatSpec with TestRunner {

  "A SerialSpamDetector2" should "run with simple stats" in {
    runSerialSpamDetector(classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
  }

  it should "run with more elaborate stats" in {
    runSerialSpamDetector(classOf[SpamDetectorStats], DataDesc.SHORT)
  }

  it should "run with ip stats" in {
    runSerialSpamDetector(classOf[IpSpamDetectorStats], DataDesc.SHORT)
  }

}
