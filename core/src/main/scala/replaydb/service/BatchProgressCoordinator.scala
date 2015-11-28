package replaydb.service

class BatchProgressCoordinator {
  trait ThreadCoordinator {
    def awaitAdvance(ts: Long): Unit
  }

  def update(phaseId: Int, maxTimestamp: Long): Unit = {

  }

  def getCoordinator(partitionId: Int, phaseId: Int): ThreadCoordinator = {
    new ThreadCoordinator() {
      override def awaitAdvance(ts: Long): Unit = {
        if (partitionId == 1) {

        }
      }
    }
  }
}
