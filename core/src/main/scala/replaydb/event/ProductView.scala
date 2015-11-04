package replaydb.event

case class ProductView(ts: Long, sku: Long) extends Event {
  override def id = sku
}

