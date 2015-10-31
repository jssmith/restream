package replaydb.language.rcollection

import replaydb.language.event.Event
import replaydb.language.Match

class MappedRCollection[S <: Event, T <: Event](parent: RCollection[S], mapF: S => T) extends RCollection[T] {
  def map[U <: Event](f: T => U): MappedRCollection[T, U] = {
    new MappedRCollection[T, U](this, f)
  }

  def count: Long = {
    parent.count
  }

  override def iterator: Iterator[Match[T]] = {
    // TODO this iterator model won't actually work... not accounting for pushing
    new Iterator[Match[T]] {
      val parentIterator = parent.iterator
      def hasNext: Boolean = {
        parentIterator.hasNext
      }
      def next(): Match[T] = {
        val mappedValues = parentIterator.next().getEvents.map(mapF)
        new Match[T](mappedValues.head, mappedValues.tail)
      }
    }
  }
}
