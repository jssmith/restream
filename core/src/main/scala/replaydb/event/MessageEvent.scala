package replaydb.event

case class MessageEvent (
  ts: Long,
  messageId: Long,
  senderUserId: Long,
  recipientUserId: Long,
  content: String
) extends Event
