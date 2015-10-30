package replaydb.language

import replaydb.language.bindings.{NamedVarBinding, NamedBinding}

package object bindings {

  // turn anything into a ConstantBinding
  implicit def valToBinding[T](v: T): Binding[T] = {
    new ConstantBinding[T](v)
  }

  implicit def stringToNamedBindingCreator[T](name: String): NamedBindingCreator[T] = {
    new NamedBindingCreator[T](name)
  }

}

class NamedBindingCreator[T](name: String) {
  def bind: NamedBinding[T] = {
    new NamedVarBinding[T](name) // TODO not correct, should be a lookup
  }
}
