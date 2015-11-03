package replaydb.util

/**
 * Simple progress updates written to standard out.
 *
 * @param printInterval interval at which to print progress, in counter updates, default 5000
 */
class ProgressMeter(val printInterval: Long = 5000, val extraInfo: () => String = () => "", val name: Option[String] = None) {
  val startTime = System.currentTimeMillis()
  var ct: Long = 0L
  var nextPrintPoint = printInterval

  var lastPrintTime = startTime
  var lastPrintCt = 0L

  private def formatName: String = {
    name match {
      case Some(s) => s"$s: "
      case None => ""
    }
  }

  def increment(): Unit = {
    add(1)
  }

  def add(delta: Int): Unit = {
    ct += delta
    if (ct >= nextPrintPoint) {
      val printTime = System.currentTimeMillis()
      println(s"""${formatName}progress $ct, current rate ${(ct - lastPrintCt)*1000/(printTime - lastPrintTime)}""")
      lastPrintTime = printTime
      lastPrintCt = ct
      val extraStr = extraInfo()
      if (extraStr != "") {
        println("  " + extraStr)
      }
      nextPrintPoint = (ct / printInterval + 1) * printInterval
    }
  }

  def finished(): Unit = {
    val elapsedTime = System.currentTimeMillis() - startTime
    println(s"""${formatName}completed $ct in $elapsedTime ms, rate of ${ct*1000/elapsedTime}/s""")
  }
}
