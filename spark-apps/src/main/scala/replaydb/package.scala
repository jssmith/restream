import scala.collection.mutable.ArrayBuffer

package object replaydb {

  private val emailRegex = """[\w-_\.+]*[\w-_\.]\@([\w]+\.)+[\w]+[\w]""".r
  def hasEmail(msg: String): Boolean = {
    emailRegex.pattern.matcher(msg).find()
  }

  type TSVal[A] = (Long, A)
  type IntPair = (Int, Int)
  type LongPair = (Long, Long)

  def addPairsOption[A](pairs: (Option[(A, A)], Option[(A, A)]))(implicit num: Numeric[A]): (A, A) = {
    pairs match {
      case (Some(a), Some(b)) => addPairs[A](a, b)
      case (Some(a), None) => a
      case (None, Some(b)) => b
      case _ => (num.zero, num.zero)
    }
  }

  def addPairs[A](pair1: (A, A), pair2: (A, A))(implicit num: Numeric[A]): (A, A) = {
    (num.plus(pair1._1, pair2._1), num.plus(pair1._2, pair2._2))
  }

  // Takes (Long, A) pairs and sorts them by the first member of
  // the tuple (the timestamp)
  class TimestampOrdering[A] extends Ordering[TSVal[A]] {
    def compare(x: (Long, A), y: (Long, A)): Int = x._1.compare(y._1)
  }

  class BufferTimestampOrdering[A] extends Ordering[ArrayBuffer[TSVal[A]]] {
    def compare(x: ArrayBuffer[TSVal[A]], y: ArrayBuffer[TSVal[A]]): Int = {
      -1 * x.head._1.compare(y.head._1)
    }
  }
}
