package replaydb.event

trait HasPartitionKey {
  def partitionKey: Long
}
