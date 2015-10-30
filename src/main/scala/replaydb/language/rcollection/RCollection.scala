package replaydb.language.rcollection

abstract class RCollection[B] extends Iterable[B] {
  def map[C](f: (B) => C): MappedRCollection[B, C]
  def count: Long
}
