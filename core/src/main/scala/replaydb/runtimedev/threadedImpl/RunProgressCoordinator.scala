package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{ReplayDelta, ReplayState}

import scala.collection.mutable

// Assuming we know the startTime in advance
class RunProgressCoordinator(numPartitions: Int, numPhases: Int, batchSize: Int, startTime: Long) {

  val finished = Array.ofDim[Boolean](numPhases + 2, numPartitions)
  val checkpoints = Array.ofDim[Int](numPhases + 2, numPartitions)
  val replayStates = mutable.Set[ReplayState]()

  for (i <- 0 until numPartitions) {
    checkpoints(0)(i) = Int.MaxValue
    checkpoints(numPhases + 1)(i) = Int.MaxValue
    finished(0)(i) = true
    finished(numPhases + 1)(i) = true
  }

  val MaxInFlightBatches = 1000

  abstract class CoordinatorInterface(partitionId: Int, phaseId: Int) {
    def reportCheckpoint(ts: Long): Long

    def reportFinished(): Unit = checkpoints.synchronized { checkpoints(phaseId)(partitionId) = Int.MaxValue; checkpoints.notifyAll() }
//    def reportFinished(): Unit = checkpoints.synchronized { finished(phaseId)(partitionId) = true; checkpoints.notifyAll() }

    def gcAllReplayState(): Unit
  }

  def registerReplayStates(states: Iterable[ReplayState]): Unit = {
    states.foreach(this.registerReplayState)
  }

  def registerReplayState(rs: ReplayState): Unit = {
    replayStates += rs
  }

  var stateToDeltasMap = mutable.Map[ReplayState, Array[Array[ReplayDelta]]]()

  def getDeltaForState(state: ReplayState, phaseId: Int): ReplayDelta = {
    val deltas = stateToDeltasMap.synchronized {
      stateToDeltasMap.getOrElseUpdate(state, Array.ofDim(numPhases + 2, MaxInFlightBatches))
    }

    val batchId = checkpoints.synchronized {
      checkpoints(phaseId)(0) % MaxInFlightBatches // assuming single partition for now
    }

    if (deltas(phaseId)(batchId) == null) {
      deltas(phaseId)(batchId) = state.getDelta
    }
    deltas(phaseId)(batchId)
  }

  def relative(ts: Long): Long = ts - startTime

  def getCoordinatorInterface(partitionId: Int, phaseId: Int): CoordinatorInterface = {
    new CoordinatorInterface(partitionId, phaseId) {
      override def reportCheckpoint(ts: Long): Long = {
        val checkpointNumber: Int = (relative(ts) / batchSize).toInt // round down; reporting in middle of batch is same as at start
        var checkpointMergeStart = checkpointNumber
        checkpoints.synchronized {
          val oldCheckpointNumber = checkpoints(phaseId)(partitionId)
          if (checkpointNumber != oldCheckpointNumber) {
            checkpointMergeStart = oldCheckpointNumber + 1
//            println(s"phase $phaseId now at chkpt $checkpointNumber")
            checkpoints(phaseId)(partitionId) = checkpointNumber
            checkpoints.notifyAll()
          }
          while (checkpointNumber - 10 /*MaxInFlightBatches*/ > checkpoints(phaseId + 1).min
//            || (!finished(phaseId - 1).forall(_ == true) && checkpointNumber >= checkpoints(phaseId - 1).min)) {
            || (checkpointNumber >= checkpoints(phaseId - 1).min)) {
//            println(s"phase $phaseId WAITing because chkpts(${phaseId + 1}).min is ${checkpoints(phaseId + 1).min}" +
//              s" and chkpts(${phaseId - 1}).min is ${checkpoints(phaseId - 1).min} but this is at chkptNum $checkpointNumber")
            val t = System.currentTimeMillis()
            checkpoints.wait(10000)
            if (System.currentTimeMillis() - t > 10000) {
              println(s"phase $phaseId waited for more than 10 seconds")
            }
          }
        }
        // Ready to move forward; state is ready
        if (phaseId != 1) {
          stateToDeltasMap.synchronized {
            for (entry <- stateToDeltasMap) entry match {
              case (rs, deltas) =>
                for (checkNum <- checkpointMergeStart to checkpointNumber) {
                  val batchID = checkNum % MaxInFlightBatches
                  val delta = deltas(phaseId - 1)(batchID)
                  if (delta == null) {
                    // do nothing
                  } else {
                    rs.merge(delta)
                  }
                }
            }
          }
        }
        startTime + batchSize.asInstanceOf[Long] * (checkpointNumber + 1)
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