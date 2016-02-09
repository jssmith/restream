package replaydb.event

case class NewFriendshipEvent (
  ts: Long,
  userIdA: Long,
  userIdB: Long
) extends Event with HasPartitionKey {
  override def partitionKey: Long = userIdA
}
