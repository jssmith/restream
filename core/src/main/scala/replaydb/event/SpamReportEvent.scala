package replaydb.event

case class SpamReportEvent (
  ts: Long,
  userId: Long,
  messageId: Long
) extends Event {
  override def id = userId
}