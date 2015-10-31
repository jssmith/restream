package replaydb.language.bindings

class NamedVarBinding[T](val name: String) extends NamedBinding[T] {

  override def toString: String = {
//    s"Binding($name)"
    name
  }

}
