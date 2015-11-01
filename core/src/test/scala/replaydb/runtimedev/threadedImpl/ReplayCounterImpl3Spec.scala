package replaydb.runtimedev.threadedImpl

class ReplayCounterImpl3Spec extends ReplayCounterImplBase[ReplayCounterImpl3] {
  override def getCounter = new ReplayCounterImpl3
  def getName = "ReplayCounterImpl3"
}
