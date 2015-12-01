package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{CoordinatorInterface, ReplayState}

import scala.collection.mutable

// Assuming we know the startTime in advance
class RunProgressCoordinator(numPartitions: Int, numPhases: Int, batchSizeGoal: Int, startTime: Long) {

  val checkpoints = Array.ofDim[Int](numPhases + 2, numPartitions)
  val replayStates = mutable.Set[ReplayState with Threaded]()

  val MaxInFlightBatches = 50
  val readUntil = Array.ofDim[Long](numPhases + 1, MaxInFlightBatches, numPartitions)

  for (i <- 0 until numPartitions) {
    checkpoints(0)(i) = Int.MaxValue
    checkpoints(numPhases + 1)(i) = Int.MaxValue
    for (j <- 0 until MaxInFlightBatches) {
      readUntil(0)(j)(i) = Long.MaxValue
    }
  }

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

          while (checkpointNumber - (MaxInFlightBatches-1) > checkpoints(phaseId + 1).min
            || (checkpointNumber >= checkpoints(phaseId - 1).min)) {
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
//          if (phaseId != 1) {
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
        val nextCheckpointTs = readUntil.synchronized {
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
        }).sum
        ReplayValueImpl.gcAvg.add(totalCollected)
      }

      // For now doing GC based off of the farthest back thread in the farthest back phase
      def getOldestTSProgressMark: Long = {
        readUntil.synchronized {
          readUntil(numPhases).map(_.min).min
        }
      }
    }
  }
}

object RunProgressCoordinator {
  // TODO probably a cleaner way to achieve this?
  def getDriverCoordinator: CoordinatorInterface = {
    new CoordinatorInterface(0, 1) {
      override def gcAllReplayState(): Unit = {
        throw new UnsupportedOperationException
      }

      override def batchId: Int = 0

      override def reportCheckpoint(ts: Long, ct: Long): (Long, Long) = {
        throw new UnsupportedOperationException
      }

      override def reportFinished(): Unit = {
        throw new UnsupportedOperationException
      }
    }
  }
}