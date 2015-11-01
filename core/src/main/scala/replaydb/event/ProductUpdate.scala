package replaydb.event

case class ProductUpdate(ts: Long, sku: Long) extends Event