package replaydb.runtimedev.distributedImpl

import org.scalatest.FlatSpec
import replaydb.runtimedev.BatchInfo
import replaydb.service.driver.RunConfiguration
import scala.language.reflectiveCalls

class ReplayMapTopKImplSpec extends FlatSpec {

  def workers(cnt: Int) = new {
    val numPhases = 4
    val runConfig = RunConfiguration(cnt, numPhases, (0 until cnt).map(_ => null).toArray, 0, 10, 5, false, true)
    val commServices = (0 until cnt).map(new LocalStateCommunicationService(_, runConfig))
    commServices.foreach(_.setWorkers(commServices.toList))
    val replayStates = (for ((commService, idx) <- commServices.zipWithIndex) yield {
      val rs = new ReplayMapTopKImpl[Long, Long](0, idx, commService)
      commService.registerReplayState(0, rs)
      rs
    }).toList
  }

  def getBI(workerNum: Int, phaseNum: Int, bets: Long) = new BatchInfo(workerNum, phaseNum) {
    override def batchEndTs = bets
  }

  def submitWrite(states: List[ReplayMapTopKImpl[Long, Long]], workerNum: Int, phaseNum: Int, batchEndTs: Long, ts: Long, key: Long, newVal: Long): Unit = {
    states(workerNum).merge(ts, key, _ => newVal)(getBI(workerNum, phaseNum, batchEndTs))
  }
  
  "Distributed ReplayMapTopK" should "correctly share across partitions 1" in {
    val w = workers(3)
    val rs0 = w.replayStates(0)
    val rs1 = w.replayStates(1)
    val rs2 = w.replayStates(2)
    val rs = w.replayStates

    submitWrite(rs, 0, 0, 10, 4, 1, 5)
    submitWrite(rs, 1, 0, 10, 18, 1, 10)

    submitWrite(rs, 2, 0, 10, 2, 2, 15)
    submitWrite(rs, 2, 0, 10, 3, 17, 2)
    
    submitWrite(rs, 0, 0, 10, 3, 3, 12)
    
    submitWrite(rs, 0, 0, 10, 17, 4, 17)
    
    submitWrite(rs, 1, 0, 10, 3, 5, 9)

    rs0.getPrepareTopK(5, 3)(getBI(0, 0, 10))
    rs1.getPrepareTopK(7, 3)(getBI(1, 0, 10))
    rs2.getPrepareTopK(9, 3)(getBI(2, 0, 10))

    w.commServices.foreach(_.finalizeBatch(0, 10))
    w.commServices.foreach(_.awaitReadsReady(1, 10))

    assert(rs0.getTopK(5, 3)(getBI(0, 1, 10)) === List((2, 15), (3, 12), (5, 9)))
    assert(rs1.getTopK(7, 3)(getBI(1, 1, 10)) === List((2, 15), (3, 12), (5, 9)))
    assert(rs2.getTopK(9, 3)(getBI(2, 1, 10)) === List((2, 15), (3, 12), (5, 9)))
  }

}
