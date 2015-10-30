package replaydb.language

package object bindings {

  implicit def valToBinding[T](v: T): Binding[T] = {
    new ConstantBinding[T](v)
  }

}
