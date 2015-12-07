package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.ReplayState

import scala.collection.mutable

// Assuming we know the startTime in advance
class RunProgressCoordinator(numPartitions: Int, numPhases: Int, batchSizeGoal: Int, startTime: Long) {

  val checkpoints = Array.ofDim[Int](numPhases, numPartitions)
  val replayStates = mutable.Set[ReplayState with Threaded]()

  val MaxInFlightBatches = 50
  val readUntil = Array.ofDim[Long](numPhases, MaxInFlightBatches, numPartitions)

  def registerReplayStates(states: Iterable[ReplayState with Threaded]): Unit = {
    states.foreach(this.registerReplayState)
  }

  def registerReplayState(rs: ReplayState with Threaded): Unit = {
    replayStates += rs
//    val array = Array.ofDim[ReplayDelta](numPartitions, numPhases + 1, MaxInFlightBatches)
//    for (partId <- 0 until numPartitions) {
//      for (pid <- 1 to numPhases) {
//        for (bid <- 0 until MaxInFlightBatches) {
//          array(partId)(pid)(bid) = rs.getDelta
//        }
//      }
//    }
//    stateToDeltasMap += ((rs, array))
  }

//  var stateToDeltasMap = immutable.Map[ReplayState with Threaded, Array[Array[Array[ReplayDelta]]]]()

  def relative(ts: Long): Long = ts - startTime

  def getCoordinatorInterface(partitionId: Int, phaseId: Int): CoordinatorInterface = {
    new CoordinatorInterface(partitionId, phaseId) {

      var currentBatchId: Int = 0

      override def reportCheckpoint(ts: Long, ct: Long): (Long, Long) = {
//        val checkpointNumber: Int = (relative(ts) / batchSizeGoal).toInt

        //        var checkpointMergeStart = checkpointNumber
        val checkpointNumber = checkpoints.synchronized {
          checkpoints(phaseId)(partitionId) + 1
        }

        readUntil.synchronized {
          readUntil(phaseId)((checkpointNumber - 1) % MaxInFlightBatches)(partitionId) = ts
        }

        checkpoints.synchronized {
          checkpoints(phaseId)(partitionId) = checkpointNumber
          checkpoints.notifyAll()

          while ((phaseId != numPhases-1 && checkpointNumber - (MaxInFlightBatches-1) > checkpoints(phaseId + 1).min)
            || (phaseId != 0 && (checkpointNumber >= checkpoints(phaseId - 1).min))) {
//            println(s"phase $phaseId WAITing because chkpts(${phaseId + 1}).min is ${checkpoints(phaseId + 1).min}" +
//              s" and chkpts(${phaseId - 1}).min is ${checkpoints(phaseId - 1).min} but this is at chkptNum $checkpointNumber")
            val t = System.currentTimeMillis()
            checkpoints.wait(10000)
            if (System.currentTimeMillis() - t > 10000) {
              println(s"phase $phaseId waited for more than 10 seconds")
            }
          }
          checkpointNumber
        }

        currentBatchId = checkpointNumber % MaxInFlightBatches
        // Ready to move forward; state is ready

//        val deltaMap = stateToDeltasMap.synchronized {
//          if (phaseId != 0) {
//            for (entry <- stateToDeltasMap) entry match {
//              case (rs, deltas) =>
//                for (part <- 0 until numPartitions) {
//                  rs.merge(deltas(part)(phaseId - 1)(batchId))
//                }
//            }
//          }
//          stateToDeltasMap.mapValues(_(partitionId)(phaseId)(batchId).asInstanceOf[ReplayState])
////          stateToDeltasMap.map(entry => (entry._1.asInstanceOf[ReplayState], entry._2(partitionId)(phaseId)(batchId)))
//        }
        val nextCheckpointTs = if (phaseId == 0) Long.MaxValue else readUntil.synchronized {
          readUntil(phaseId - 1)(currentBatchId).min
        } - 1
        (nextCheckpointTs, ct + batchSizeGoal)
      }

      def batchId: Int = currentBatchId

      override def reportFinished(): Unit = {
        val checkpointNumber = checkpoints.synchronized {
          checkpoints(phaseId)(partitionId)
        }
        readUntil.synchronized {
          readUntil(phaseId)(checkpointNumber % MaxInFlightBatches)(partitionId) = Long.MaxValue
        }
        checkpoints.synchronized {
          checkpoints(phaseId)(partitionId) = Int.MaxValue
          checkpoints.notifyAll()
        }
      }

      override def gcAllReplayState(): Unit = {
        val ts = getOldestTSProgressMark
        val totalCollected = (for (rs <- replayStates) yield {
          rs.gcOlderThan(ts)
        }).map(_._4).sum
        ReplayValueImpl.gcAvg.add(totalCollected)
      }

      // For now doing GC based off of the farthest back thread in the farthest back phase
      def getOldestTSProgressMark: Long = {
        readUntil.synchronized {
          readUntil(numPhases - 1).map(_.min).min
        }
      }
    }
  }
}