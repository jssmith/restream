package replaydb.exec

import replaydb.util.time._

package object spam {

  val SpamScoreThreshold = 10
  val EmailRegex = """\b[A-z0-9._%+-]+@[A-z0-9.-]+\.[A-z]{2,}\b""".r.unanchored

  def containsEmail(message: String): Boolean = {
    message match {
      case EmailRegex(_*) => true
      case _ => false
    }
  }

  // RULE 2: parameters for matching # of messages sent to nonfriends in certain interval
  val NonfriendMessageCountSpamThreshold = 7
  val UniqueNonfriendCountSpamThreshold = 5
  val NonfriendMessageCountInterval = 120.minutes

  // RULE 3: parameters for matching fraction of messages sent containing an email address
  val MessageContainingEmailInterval = 30.days
  val MessageContainingEmailFractionThreshold = 0.5

  // RULE 4: parameters for fraction of last 7 days' messages sent in last 24 hours
  val MessagesLast7DaysSentLast24HoursFraction = 0.2

  // RULE 5: parameter for fraction of messages overall sent as response (defined as
  //         a message A->B when a message B->A exists within the last 7 days)
  val MessageSentInResponseFractionThreshold = 0.1

}
