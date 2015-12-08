package replaydb.exec.spam

object SpamUtil {
  private val emailRegex = """[\w-_\.+]*[\w-_\.]\@([\w]+\.)+[\w]+[\w]""".r
  def hasEmail(msg: String): Boolean = {
    emailRegex.pattern.matcher(msg).find()
  }
}
