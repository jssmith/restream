package replaydb.runtimedev.threadedImpl

class ReplayCounterImplSpec extends ReplayCounterImplBase[ReplayCounterImpl] {
  override def getCounter = new ReplayCounterImpl
  def getName = "ReplayCounterImpl3"
}
