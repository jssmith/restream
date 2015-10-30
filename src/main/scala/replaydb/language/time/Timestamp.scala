package replaydb.language.time

object Timestamp {

  val TimestampAny = new Timestamp(0L) {
    // equal to everything?
    override def equals(a: Any): Boolean = {
      a.isInstanceOf[Timestamp]
    }

    override def toString: String = "anyTime"
  }

  val TimestampNow = new Timestamp(0L) {

  }

  def now: Timestamp = {
    new Timestamp(0L) // TODO return system's current definition of 'now'
  }

  def any = { TimestampAny }

  implicit def timestampToLong(ts: Timestamp): Long = {
    ts.ts
  }
}

class Timestamp(val ts: Long) {

  override def equals(a: Any): Boolean = {
    if (a == Timestamp.TimestampAny)
      true
    else
      ts == a.asInstanceOf[Timestamp].ts
  }

  override def toString: String = {
    ts.toString
  }
}