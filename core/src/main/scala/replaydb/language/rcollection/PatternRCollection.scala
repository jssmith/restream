package replaydb.language.rcollection

import replaydb.language.Match
import replaydb.language.event.Event
import replaydb.language.pattern.Pattern

class PatternRCollection[T <: Event](parent: Pattern[T]) extends RCollection[T] {

  override def map[B <: Event](f: T => B): MappedRCollection[T, B] = {
    new MappedRCollection[T, B](this, f)
  }

  override def count: Long = {
    parent.getMatches.size
  }

  override def iterator: Iterator[Match[T]] = {
    val parentSet = parent.getMatches
//    parentSet.iterator
    null
  }
}
