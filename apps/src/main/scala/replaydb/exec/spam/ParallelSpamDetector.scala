package replaydb.exec.spam

import replaydb.runtimedev.{PrintSpamCounter, ReplayState, MemoryStats}
import replaydb.runtimedev.threadedImpl.{ReplayStateFactory, MultiReaderEventSource, RunProgressCoordinator}
import replaydb.util.ProgressMeter
import replaydb.util

object ParallelSpamDetector extends App {

  if (args.length != 4) {
    println(
      """Usage: ParallelSpamDetector baseFilename numPartitions batchSize gcInterval
        |  Suggested values: numPartitions = 4, batchSize = 500, gcInterval = 50000
      """.stripMargin)
    System.exit(1)
  }

  val partitionFnBase = args(0)
  val numPartitions = args(1).toInt
  val batchSize = args(2).toInt
  val gcInterval = args(3).toInt

  val startTime = util.Date.df.parse("2015-01-01 00:00:00.000").getTime

  val stats = new SpamDetectorStats(new ReplayStateFactory)
  val si = stats.getRuntimeInterface
  val numPhases = si.numPhases

  val barrier = new RunProgressCoordinator(numPartitions = numPartitions, numPhases = numPhases, batchSizeGoal = batchSize, startTime = startTime)

  barrier.registerReplayStates(stats.getAllReplayStates)
  val overallProgressMeter = new ProgressMeter(1000000, name = Some("Overall Progress"))
  val readerThreads = (for (partitionId <- 0 until numPartitions) yield {
    new MultiReaderEventSource(s"$partitionFnBase-$partitionId", numPhases, bufferSize = 100000)
  }).toArray
  val threads =
    for (partitionId <- 0 until numPartitions; phaseId <- 1 to si.numPhases) yield {
      new Thread(new Runnable {
        implicit val b = barrier.getCoordinatorInterface(partitionId, phaseId)
        override def run(): Unit = {
          var lastTimestamp = 0L
          val pm = new ProgressMeter(printInterval = 1000000, () => s"${MemoryStats.getStats()}", name = Some(s"$partitionId-$phaseId"))
          var ct = 0L
          var nextCheckpointTs = 0L
          var nextCheckpointCt = 0L
          readerThreads(partitionId).readEvents(e => {
            while (e.ts > nextCheckpointTs || ct >= nextCheckpointCt) {
              val nextCheckpoint = b.reportCheckpoint(e.ts, ct)
              nextCheckpointTs = nextCheckpoint._1
              nextCheckpointCt = nextCheckpoint._2
            }
            si.update(e)
            ct += 1
            if (ct % batchSize == 0 && phaseId == numPhases) {
              overallProgressMeter.synchronized { overallProgressMeter.add(batchSize) }
            }
            lastTimestamp = e.ts
            pm.increment()
            if (partitionId == numPartitions - 1 && phaseId == numPhases) {
              if (ct % gcInterval == 0) {
                b.gcAllReplayState()
              }
              if (ct % 500000 == 0) {
                si.update(new PrintSpamCounter(lastTimestamp))
              }
            }
          })
          if (phaseId == numPhases) {
            overallProgressMeter.synchronized { overallProgressMeter.add((ct % batchSize).toInt) }
          }
          b.reportFinished()
          pm.finished()
        }
      }, s"process-events-$partitionId-$phaseId")
    }
  readerThreads.foreach(_.start())
  threads.foreach(_.start())
  readerThreads.foreach(_.join())
  threads.foreach(_.join())
  overallProgressMeter.synchronized{ overallProgressMeter.finished() }

  // TODO should support an aggregate / counter type that is write-only during
  // execution ( + commutative/associative) and then you can access at the end
  // -> the pause right now to roll-up all of the spamcounters is completely unnecessary
  println("Final spam count: " + stats.spamCounter.get(Long.MaxValue, 0)(RunProgressCoordinator.getDriverCoordinator))
}
