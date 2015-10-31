package replaydb.language.rcollection

import replaydb.language.Match
import replaydb.language.event.Event

abstract class RCollection[T <: Event] extends Iterable[T] {
  def map[S](f: T => S): MappedRCollection[T, S]
  def count: Long
  def iterator: Iterator[Match[T]]
}
