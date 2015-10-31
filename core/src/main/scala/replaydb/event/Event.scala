package replaydb.event

/**
 * Base trait for events
 */
trait Event {
  /**
   * Timestamp of this event, in milliseconds of the Unix epoch
   * @return
   */
  def ts: Long
}

object Event {
  /**
   * Companion object provides ordering to sort events chronologically
   */
  implicit val eventOrdering = new Ordering[Event] {
    override def compare(x: Event, y: Event): Int = x.ts compare y.ts
  }
}