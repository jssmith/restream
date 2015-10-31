package replaydb.language.bindings

trait NamedBinding[T] extends Binding[T] {

  def name: String

}
