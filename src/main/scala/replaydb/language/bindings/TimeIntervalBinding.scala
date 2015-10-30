package replaydb.language.bindings

import replaydb.language.time.{Timestamp, TimeOffset}

class TimeIntervalBinding(relativeTo: NamedBinding[Timestamp], min: TimeOffset, max: TimeOffset, name: String = null) extends Binding[Timestamp] {

  override def toString: String = {
    s"between $min and $max from ${relativeTo.name}"
  }

}
