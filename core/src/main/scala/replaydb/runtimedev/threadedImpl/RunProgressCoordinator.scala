package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayState

import scala.collection.mutable

class RunProgressCoordinator(numPartitions: Int, numPhases: Int, maxInProgressEvents: Int) {
  trait CoordinatorInterface {
    def update(ts: Long, ct: Long): Unit
    def requestProgress(ts: Long, ct: Long): Long
    def finished(): Unit = update(Long.MaxValue, Long.MaxValue)

    def gcAllReplayState(): Unit
  }

  val timeProgressMarks = Array.ofDim[Long](numPhases, numPartitions)
  val ctProgressMarks = Array.ofDim[Long](numPhases, numPartitions)
  val replayStates = mutable.Set[ReplayState]()

  def registerReplayStates(states: Iterable[ReplayState]): Unit = {
    states.foreach(this.registerReplayState)
  }

  def registerReplayState(rs: ReplayState): Unit = {
    replayStates += rs
  }

  def getCoordinatorInterface(partitionId: Int, phaseId: Int): CoordinatorInterface = {
    new CoordinatorInterface {
      override def requestProgress(ts: Long, ct: Long): Long = {
        // TODO: should we also call update(ts-1) or require that explicitly?
        if (phaseId == 0) {
          Long.MaxValue
        } else {
          timeProgressMarks.synchronized {
            var minSafeTs = 0L
            while ({ minSafeTs = timeProgressMarks(phaseId - 1).min; ts >= minSafeTs }) {
//              println(s"$partitionId-$phaseId awaiting $ts at $minSafeTs")
              timeProgressMarks.wait()
            }
            minSafeTs
          }
        }
      }
      override def update(maxSafeTs: Long, ct: Long): Unit = {
        timeProgressMarks.synchronized {
//          println(s"$partitionId-$phaseId updating with $maxSafeTs")
          timeProgressMarks(phaseId)(partitionId) = maxSafeTs
          timeProgressMarks.notifyAll()
        }
        ctProgressMarks.synchronized {
          ctProgressMarks(phaseId)(partitionId) = ct
          ctProgressMarks.notifyAll()
          if (phaseId + 1 < numPhases) {
            var minFollowCt = 0L
            while ({ minFollowCt = ctProgressMarks(phaseId + 1).min; minFollowCt < ct - maxInProgressEvents / numPartitions }) {
              ctProgressMarks.wait()
            }
          }
        }
      }

      override def gcAllReplayState(): Unit = {
        val ts = getOldestTSProgressMark
        val totalCollected = (for (rs <- replayStates) yield {
          rs.gcOlderThan(ts)
        }).sum
        ReplayValueImpl.gcAvg.add(totalCollected)
      }

      // For now doing GC based off of the farthest back thread in the farthest back phase
      def getOldestTSProgressMark: Long = {
        timeProgressMarks.synchronized {
          timeProgressMarks.map(_.min).min
        }
      }
    }
  }
}
