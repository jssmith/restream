package replaydb.exec.spam.runAnalysis

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object BatchPathAnalysis extends App {
  val fn = args(0)
  val startTime = args(1).toLong
  val timeInterval = args(2).toDouble
  val batchSize = args(3).toInt
  val partitions = args(4).toInt

  val batchTimeInterval = (timeInterval * batchSize).toLong
  val startPrinting = startTime
//  val startPrinting = startTime + batchTimeInterval + (timeInterval*500000/partitions).toLong

  def analyze(): Unit = {
    if (batchEndTs > startPrinting) {
      val analysis = PathAnalysis(edges)
      println(s"$partitions $fn $batchEndTs: $analysis")
      printCt += 1
      if (printCt > 5) {
        System.exit(0)
      }
    } else {
      println(s"skip $batchEndTs waiting for $startPrinting")
    }
    edges.clear()
    batchEndTs += batchTimeInterval
    batchIndex += 1
  }

  var batchEndTs = startTime + batchTimeInterval
  var batchIndex = 0
  var printCt = 0
  val edges = ArrayBuffer[(Long,Long)]()

  var lastSrc = Long.MinValue
  var lastDst = Long.MinValue
  for (line <- Source.fromFile(fn).getLines()) {
    val lineparts = line.split(",")
    if (lineparts.length != 2) {
      throw new RuntimeException("unexpected length")
    }
    val src = lineparts(0).toLong
    val dst = lineparts(1).toLong
    if (src >= dst) {
      throw new RuntimeException(s"invalid pair $src $dst")
    }
    if (dst < lastDst) {
      throw new RuntimeException(s"decreasing dst $lastDst to $dst")
    }

    if (dst >= batchEndTs) {
      analyze()
    }
    if (!(lastSrc == src && lastDst == dst)) {
      edges += (src -> dst)
    }
    lastDst = dst
    lastSrc = src

  }
  println("end of file reached")

}
