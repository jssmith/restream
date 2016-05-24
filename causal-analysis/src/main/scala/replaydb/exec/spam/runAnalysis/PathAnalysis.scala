package replaydb.exec.spam.runAnalysis

import edu.princeton.cs.algs4._

import scala.collection.JavaConversions._
import scala.collection.immutable.{HashMap, HashSet}

case class PathAnalysis(criticalPathLength: Int, numEvents: Int) {
  override def toString(): String = {
    s"$numEvents events, critical path length $criticalPathLength"
  }
}

object PathAnalysis {
  def apply(edges: Iterable[(Long,Long)]) = {
    val sourceVertices = HashSet[Long]() ++ edges.map(_._1)
    val dstVertices = HashSet[Long]() ++ edges.map(_._2)
    val allVertices = HashSet[Long]() ++ edges.map(_._1) ++ edges.map(_._2)
    val srcOnlyVertices = sourceVertices.diff(dstVertices)
    val intMapping = HashMap[Long,Int]() ++ allVertices.toList.zipWithIndex
    val nVertices = allVertices.size
    val digraph = new Digraph(nVertices)
    for ((src, dst) <- edges) {
      digraph.addEdge(intMapping(src), intMapping(dst))
    }

    val t = new Topological(digraph)
    if (!t.hasOrder) {
      System.err.println("Order is not available")
      System.exit(1)
    }

    val distances = new Array[Int](nVertices)
    (0 until nVertices).foreach(distances(_) = Integer.MIN_VALUE)
    srcOnlyVertices.foreach(id => distances(intMapping(id)) = 0)
    for (u <- t.order()) {
      for (v <- digraph.adj(u)) {
        if (distances(v) < distances(u) + 1) {
          distances(v) = distances(u) + 1
        }
      }
    }
    val criticalLength = distances.max
    new PathAnalysis(criticalLength, nVertices)
  }
}
