package replaydb.experiment

object Timestamp {

  val TIMESTAMP_ANY = new Timestamp(0L) {
    // equal to everything?
    override def equals(a: Any): Boolean = {
      a.isInstanceOf[Timestamp]
    }
  }

  def now: Timestamp = {
      new Timestamp(0L) // return system's current definition of 'now'
  }

  def any: Timestamp = { TIMESTAMP_ANY }

  implicit def timestampToLong(ts: Timestamp): Long = {
    ts.ts
  }
}

class Timestamp(val ts: Long) {

  override def equals(a: Any): Boolean = {
    if (a == Timestamp.TIMESTAMP_ANY)
      true
    else
      ts == a.asInstanceOf[Timestamp].ts
  }

  override def toString: String = {
    ts.toString
  }
}