package replaydb.exec.spam

import org.scalatest.FlatSpec
import replaydb.util.MemoryStats
import replaydb.service.Server

/**
 *
 * Example commands for generating datafiles:
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-short-split-1/events.out 1
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-short-split-2/events.out 2
 * runMain replaydb.exec.EventGenerator tunable 100000 500000 /Users/johann/tmp/events-short-split-4/events.out 4
 *
 * runMain replaydb.exec.EventGenerator tunable 100000 5000000 /Users/johann/tmp/events-split-1/events.out 1
 * runMain replaydb.exec.EventGenerator tunable 100000 5000000 /Users/johann/tmp/events-split-2/events.out 2
 * runMain replaydb.exec.EventGenerator tunable 100000 5000000 /Users/johann/tmp/events-split-4/events.out 4
 *
 */
class DistributedSpamDetectorSpec extends FlatSpec {

  private def runTest(numServers: Int, short: Boolean) = {
    val startPort = 5567
    val ports = startPort until (startPort + numServers)
    val servers = ports.map(new Server(_))
    servers.foreach(_.run())
    try {
      DistributedSpamDetector.main(Array[String](
        s"${System.getProperty("user.home")}/tmp/events-${if (short) "short-" else ""}split-$numServers/events.out",
        s"$numServers", "50000", s"src/test/resources/hosts-$numServers.txt")
      )
    } finally {
      // TODO shutdown doesn't seem to work properly
//      servers.foreach(_.close())
    }
  }

//  "A distributed spam detector" should "run with a single split" in {
//    runTest(1, true)
//  }

  it should "run with a two-way split" in {
    println(MemoryStats.getMemoryStats())
    runTest(2, true)
    println(MemoryStats.getMemoryStats())
  }

//  it should "run with a four-way split" in {
//    runTest(4, true)
//  }
}
