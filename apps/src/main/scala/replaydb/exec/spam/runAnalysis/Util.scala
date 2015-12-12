package replaydb.exec.spam.runAnalysis

import java.io.File
import scala.io.Source

protected object Util {
  def readFully(f: File): String = {
    Source.fromFile(f).getLines().mkString("\n")
  }
}
