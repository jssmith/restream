package replaydb.language.rcollection

class MappedRCollection[A, B](parent: RCollection[A], mapF: (A) => B) extends RCollection[B] {
  def map[C](f: (B) => C): MappedRCollection[B, C] = {
    new MappedRCollection[B, C](this, f)
  }
  def count: Long = {
    parent.count
  }
  def iterator: Iterator[B] = {
    new Iterator[B] {
      val parentIterator = parent.iterator
      def hasNext: Boolean = {
        parentIterator.hasNext
      }
      def next(): B = {
        mapF(parentIterator.next())
      }
    }
  }
}
