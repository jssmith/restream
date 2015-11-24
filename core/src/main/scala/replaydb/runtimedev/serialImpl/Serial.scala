package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayDelta, ReplayState}

protected trait Serial extends ReplayState {

  override def merge(delta: ReplayDelta): Unit = {
    // nothing to be done
  }

  override def gcOlderThan(ts: Long): Int = {
    0 // nothing to be done
  }

}
