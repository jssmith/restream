package replaydb.util

import java.text.SimpleDateFormat
import java.util.TimeZone

object Date {
  private def dateFormat(fmt: String) = {
    val df = new SimpleDateFormat(fmt)
    df.setTimeZone(TimeZone.getTimeZone("GMT"))
    df
  }

  val df = dateFormat("yyyy-MM-dd HH:mm:ss.SSS")

}
