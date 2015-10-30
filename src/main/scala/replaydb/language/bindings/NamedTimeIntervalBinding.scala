package replaydb.language.bindings

import replaydb.language.time.{TimeOffset, Timestamp}

class NamedTimeIntervalBinding(val name: String, relativeTo: NamedBinding[Timestamp],
                               min: TimeOffset, max: TimeOffset)
  extends TimeIntervalBinding(relativeTo, min, max) with NamedBinding[Timestamp] {

  override def toString: String = {
    super.toString + s" bound to $name"
  }

}
