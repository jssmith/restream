package replaydb.event

case class NewFriendshipEvent (
  ts: Long,
  userIdA: Long,
  userIdB: Long
) extends Event {
  override def id = userIdA
}
