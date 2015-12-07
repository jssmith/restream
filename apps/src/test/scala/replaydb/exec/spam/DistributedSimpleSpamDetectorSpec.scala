package replaydb.exec.spam

import org.scalatest.FlatSpec
import replaydb.service.Server
import replaydb.util.MemoryStats

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-1/events.out 1
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-2/events.out 2
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-4/events.out 4
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-8/events.out 8
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-12/events.out 12
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-500k-split-16/events.out 16
 *
 */
class DistributedSimpleSpamDetectorSpec extends FlatSpec with TestRunner {

//  it should "run with a one-way split" in {
//    runSpamDetector(1, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
//  }

//  it should "run with a two-way split" in {
//    runSpamDetector(2, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
//  }

//  it should "run with a four-way split" in {
//    runSpamDetector(4, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
//  }
//
  it should "run with an eight-way split" in {
    runSpamDetector(8, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
  }

//  it should "run with a twelve-way split" in {
//    runSpamDetector(12, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
//  }

//  it should "run with a sixteen-way split" in {
//    runSpamDetector(16, classOf[SimpleSpamDetectorStats], DataDesc.SHORT)
//  }

}
