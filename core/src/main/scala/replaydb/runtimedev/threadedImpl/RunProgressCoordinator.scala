package replaydb.runtimedev.threadedImpl

class RunProgressCoordinator(numPartitions: Int, numPhases: Int, maxEventsPerPhase: Int) {
  trait CoordinatorInterface {
    def update(ts: Long, ct: Long): Unit
    def requestProgress(ts: Long, ct: Long): Long
    def finished(): Unit = update(Long.MaxValue, Long.MaxValue)
  }

  val timeProgressMarks = Array.ofDim[Long](numPhases, numPartitions)
  val ctProgressMarks = Array.ofDim[Long](numPhases, numPartitions)

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
            while ({ minFollowCt = ctProgressMarks(phaseId + 1).min; minFollowCt < ct - maxEventsPerPhase / numPartitions }) {
              ctProgressMarks.wait()
            }
          }
        }
      }
    }
  }
}
