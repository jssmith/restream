package replaydb.exec.spam

class UserPair(val a: Long, val b: Long) {
  override def equals(o: Any) = o match {
    case that: UserPair => (a == that.a && b == that.b) || (a == that.b && b == that.a)
    case _ => false
  }
  override def hashCode(): Int = {
    a.hashCode() + 25741 * b.hashCode()
  }
}
