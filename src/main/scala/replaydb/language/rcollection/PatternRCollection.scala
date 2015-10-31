package replaydb.language.rcollection

import replaydb.language.event.Event
import replaydb.language.pattern.Pattern

class PatternRCollection[T <: Event](parent: Pattern[T]) extends RCollection[T] {

  // should be less restrictive
  def map[B](f: T => B): MappedRCollection[T, B] = {
    new MappedRCollection[T, B](this, f)
  }

  def count: Long = {
    parent.getMatches.size
  }

  def iterator: Iterator[T] = {
    val parentSet = parent.getMatches
//    parentSet.iterator
    null
  }
}
