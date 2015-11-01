package replaydb.runtimedev.threadedImpl

class ReplayCounterImpl2Spec extends ReplayCounterImplBase[ReplayCounterImpl2] {
  override def getCounter = new ReplayCounterImpl2
  def getName = "ReplayCounterImpl2"
}
