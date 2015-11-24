package replaydb.runtimedev.serialImpl

import replaydb.runtimedev.{ReplayDelta, ReplayValue}

class ReplayValueDelta[T](parentValue: ReplayValueImpl[T]) extends ReplayValue[T] with ReplayDelta {

  override def getOption(ts: Long): Option[T] = {
    throw new UnsupportedOperationException
  }

  override def merge(ts: Long, value: T => T): Unit = {
    parentValue.merge(ts, value)
  }
  
  override def clear(): Unit = {
    // Nothing to be done
  }

}
