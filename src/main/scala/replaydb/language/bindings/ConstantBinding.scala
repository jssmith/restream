package replaydb.language.bindings

class ConstantBinding[T](val value: T) extends Binding[T] {

  override def equals(other: Any): Boolean = other match {
    case c: ConstantBinding[_] => value == c.value
    case _ => false
  }

  override def toString: String = {
    value.toString
  }
}
