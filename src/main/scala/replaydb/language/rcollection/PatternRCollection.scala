package replaydb.language.rcollection

import replaydb.language.event.Event
import replaydb.language.pattern.Pattern

class PatternRCollection(parent: Pattern) extends RCollection[Event] {

  // should be less restrictive
  def map[B](f: (Event) => B): MappedRCollection[Event, B] = {
    new MappedRCollection[Event, B](this, f)
  }

  def count: Long = {
    parent.get_matches.size
  }

  def iterator: Iterator[Event] = {
    val parentSet = parent.get_matches
//    parentSet.iterator
    null
  }
}
