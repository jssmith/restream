package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayState

import scala.collection.mutable

// Assuming we know the startTime in advance
class RunProgressCoordinator(numPartitions: Int, numPhases: Int, batchSize: Int, startTime: Long) {

  val finished = Array.ofDim[Boolean](numPhases + 2, numPartitions)
  val checkpoints = Array.ofDim[Long](numPhases + 2, numPartitions)
  val replayStates = mutable.Set[ReplayState]()

  for (i <- 0 until numPartitions) {
    checkpoints(0)(i) = Long.MaxValue
    checkpoints(numPhases + 1)(i) = Long.MaxValue
    finished(0)(i) = true
    finished(numPhases + 1)(i) = true
  }

  val MaxInFlightBatches = 20

  abstract class CoordinatorInterface(partitionId: Int, phaseId: Int) {
    def reportCheckpoint(ts: Long, readerLock: AnyRef): Long

    def reportFinished(): Unit = checkpoints.synchronized { checkpoints(phaseId)(partitionId) = Long.MaxValue; checkpoints.notifyAll() }
//    def reportFinished(): Unit = checkpoints.synchronized { finished(phaseId)(partitionId) = true; checkpoints.notifyAll() }

    def gcAllReplayState(): Unit
  }

  def registerReplayStates(states: Iterable[ReplayState]): Unit = {
    states.foreach(this.registerReplayState)
  }

  def registerReplayState(rs: ReplayState): Unit = {
    replayStates += rs
  }

//  var mapToDeltasMap = Map[ReplayMapImpl, Array[Array[ReplayMapDelta]]]()
//
//  def getDeltaForMap(map: ReplayMapImpl, phaseId: Int): ReplayMapDelta = {
//    val deltas = mapToDeltasMap.getOrElse(map, Array.ofDim(numPhases, MaxInFlightBatches))
//    val batchId = checkpoints(phaseId)(0) // assuming single partition for now
//    if (deltas(phaseId)(batchId) == null) {
//      deltas(phaseId)
//    }
//  }

  def relative(ts: Long): Long = ts - startTime

  def getCoordinatorInterface(partitionId: Int, phaseId: Int): CoordinatorInterface = {
    new CoordinatorInterface(partitionId, phaseId) {
      override def reportCheckpoint(ts: Long, readerLock: AnyRef): Long = {
        val checkpointNumber: Long = relative(ts) / batchSize // round down; reporting in middle of batch is same as at start
        checkpoints.synchronized {
          val oldCheckpointNumber = checkpoints(phaseId)(partitionId)
          if (checkpointNumber != oldCheckpointNumber) {
//            println(s"phase $phaseId now at chkpt $checkpointNumber")
            checkpoints(phaseId)(partitionId) = checkpointNumber
            checkpoints.notifyAll()
          }
          while (checkpointNumber - MaxInFlightBatches > checkpoints(phaseId + 1).min
//            || (!finished(phaseId - 1).forall(_ == true) && checkpointNumber >= checkpoints(phaseId - 1).min)) {
            || (checkpointNumber >= checkpoints(phaseId - 1).min)) {
//            println(s"phase $phaseId WAITing because chkpts(${phaseId + 1}).min is ${checkpoints(phaseId + 1).min}" +
//              s" and chkpts(${phaseId - 1}).min is ${checkpoints(phaseId - 1).min} but this is at chkptNum $checkpointNumber")
            checkpoints.wait()
          }
        }
        startTime + batchSize * (checkpointNumber + 1)
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
        startTime + batchSize * checkpoints.synchronized {
          checkpoints.map(_.min).min
        }
      }
    }
  }
}
