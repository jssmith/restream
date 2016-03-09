package replaydb.runtimedev.threadedImpl

import replaydb.runtimedev.{BatchInfo, ReplayMapTopK}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class ReplayMapTopKImpl[K, V](default: V, maxCount: Int)
                             (implicit ord: Ordering[V], ct: ClassTag[V])
  extends ReplayMapImpl[K, V](default) with ReplayMapTopK[K, V] {

  val pairOrd = new Ordering[(K, V)] { def compare(a: (K, V), b: (K, V)) = ord.compare(a._2, b._2) }
  val topK = new ReplayValueImpl[Array[(K, V)]](Array())

  // NOTE: if count > maxCount, only the top maxCount are returned
  override def getTopK(ts: Long, count: Int)(implicit batchInfo: BatchInfo): TopK = {
    topK.get(ts) match {
      case Some(top) => top.toList.take(count)
      case None => List()
    }
  }

  def insertIntoArray(arr: Array[(K, V)], value: (K, V)): Array[(K, V)] = {
    if (arr.length == 0) {
      Array(value)
    } else if (pairOrd.gt(value, arr.head)) {
      arr.+:(value)
    } else if (pairOrd.lteq(value, arr.last)) {
      arr.:+(value)
    } else {
      val idx = arr.indexWhere(pairOrd.lt(_, value))
      (arr.slice(0, idx) :+ value) ++: arr.slice(idx, arr.length)
    }
  }

  override def merge(ts: Long, key: K, fn: (V) => V)(implicit batchInfo: BatchInfo): Unit = {
    super.merge(ts, key, fn)
    topK.merge(ts, (oldTop: Array[(K, V)]) => {
      val oldVal = oldTop.find(_._1 == key)
      val top = oldTop.filterNot(_._1 == key)
      val newValue = key -> m.get(key).get(ts).get
      val insertValue = if (oldVal.nonEmpty && pairOrd.lt(newValue, oldVal.get)) {
        // Key used to be present, and it just decreased
        // We need to search through everything to check if another value is higher
        var max = newValue
        for ((k, replayV) <- m) {
          replayV.get(ts) match {
            case Some(v) =>
              val potential = k -> v
              if (pairOrd.gt(potential, max) && !oldTop.contains(potential)) {
                max = potential
              }
            case None =>
          }
        }
        max
      } else {
        newValue
      }
      if (top.length < maxCount) {
        insertIntoArray(top, insertValue)
      } else {
        if (pairOrd.gt(insertValue, top.last)) {
          insertIntoArray(top.slice(0, top.length-1), insertValue)
        } else {
          top
        }
      }
    })
  }

  override def getPrepareTopK(ts: Long, k: Int)(implicit batchInfo: BatchInfo): Unit = {
    // Nothing to be done
  }

  override def gcOlderThan(ts: Long): (Int, Int, Int, Int) = {
    topK.gcOlderThan(ts)
    super.gcOlderThan(ts)
  }

}
