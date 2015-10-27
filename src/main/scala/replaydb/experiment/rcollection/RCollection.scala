package replaydb.experiment.rcollection

/**
 * Created by erik on 10/27/15.
 */
abstract class RCollection[B] extends Iterable[B] {
  def map[C](f: (B) => C): MappedRCollection[B, C]
  def count: Long
}
