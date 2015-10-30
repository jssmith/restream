package replaydb.language.bindings

trait Binding[T] {


//  override def toString: String = {
//    s"Binding($name)"
//  }
}

// TODO would be nice to have it such that you could just type
// Binding("A") twice and have it return the same binding each
// time, but then you need to deal with scoping...

object Binding {

  def apply[T](name: String): NamedVarBinding[T] = {
    new NamedVarBinding[T](name)
  }

  // ????
  val TimeNowBinding = new TimeNowBinding
  def now: NamedTimeIntervalBinding = { TimeNowBinding }

}




