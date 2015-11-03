package replaydb.runtimedev.threadedImpl

class RunProgressCoordinator(numPartitions: Int, numPhases: Int) {
  trait CoordinatorInterface {
    def update(ts: Long): Unit
    def requestProgress(ts: Long): Long
    def finished(): Unit = update(Long.MaxValue)
  }

  val progressMarks = Array.ofDim[Long](numPhases, numPartitions)

  def getCoordinatorInterface(partitionId: Int, phaseId: Int): CoordinatorInterface = {
    new CoordinatorInterface {
      override def requestProgress(ts: Long): Long = {
        // TODO: should we also call update(ts-1) or require that explicitly?
        if (phaseId == 0) {
          Long.MaxValue
        } else {
          progressMarks.synchronized {
            var minSafeTs = 0L
            while ({ minSafeTs = progressMarks(phaseId - 1).min; ts >= minSafeTs }) {
//              println(s"$partitionId-$phaseId awaiting $ts at $minSafeTs")
              progressMarks.wait()
            }
            minSafeTs
          }
        }
      }
      override def update(maxSafeTs: Long): Unit = {
        progressMarks.synchronized {
//          println(s"$partitionId-$phaseId updating with $maxSafeTs")
          progressMarks(phaseId)(partitionId) = maxSafeTs
          progressMarks.notifyAll()
        }
      }
    }
  }
}
