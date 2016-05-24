package replaydb.exec.spam.runAnalysis

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object BasicPathAnalysis extends App {
  val fn = args(0)
  val edges = ArrayBuffer[(Long,Long)]()
  for (line <- Source.fromFile(fn).getLines()) {
    val lineparts = line.split(",")
    if (lineparts.length != 2) {
      throw new RuntimeException("unexpected length")
    }
    val src = lineparts(0).toLong
    val dst = lineparts(1).toLong
    edges += (src -> dst)
  }
  val analysis = PathAnalysis(edges)
  println(analysis)
}
