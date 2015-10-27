package replaydb.experiment.rcollection

import replaydb.experiment.event.Event
import replaydb.experiment.pattern.Pattern

/**
 * Created by erik on 10/27/15.
 */
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
    parentSet.iterator
  }
}
