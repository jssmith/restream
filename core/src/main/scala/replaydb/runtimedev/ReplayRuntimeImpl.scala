package replaydb.runtimedev

import replaydb.event.Event

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.macros.blackbox.Context

class ReplayRuntimeImpl(val c: Context) {
  import c.universe._

  def replaceTransform(src: Name, tgt: Name) =  new Transformer {
    override def transform(tree: Tree): Tree = tree match {
      case id: Ident =>
        if (id.name == src) {
          Ident(tgt)
        } else {
          super.transform(tree)
        }
      case _ => {
        super.transform(tree)
      }
    }
  }

  def bindImpl(x: c.Expr[Any]): c.Expr[Unit] = {
    ReplayRuntime.addBinding(c.internal.enclosingOwner.owner, x)
    c.Expr[Unit](q"")
  }

  def emitImpl(x: Expr[Any])(bindings: Expr[Any]): c.Expr[Any] = {
    val bindings = ReplayRuntime.getBindings(c.internal.enclosingOwner.owner).
      asInstanceOf[ArrayBuffer[c.Expr[Any]]]
    val m = mutable.HashMap[Type, ArrayBuffer[(TermName,Tree)]]()
    for (b <- bindings) {
      b.tree match {
        case Function(params, body) =>
          if (params.size == 1 && params(0).tpt.tpe <:< typeOf[Event]) {
            m.getOrElseUpdate(params(0).tpt.tpe, ArrayBuffer[(TermName,Tree)]()) += ((params(0).name, body))
          } else {
            throw new RuntimeException("unrecognized function with parameters " + params)
          }
        case _ => throw new RuntimeException("expected function")
      }
    }
    val cases = m.map{ case (tpt, trees) =>
      val statements = trees.map { case(termName, body) =>
        val rt = replaceTransform(termName, TermName("zz"))
        rt.transform(body)
      }
      cq"zz : $tpt => ..$statements".asInstanceOf[CaseDef]
    }.toList ++ List(cq"_ => ".asInstanceOf[CaseDef])
    val me = Match(x.tree, cases)
    val res = c.Expr[Unit](c.untypecheck(me))
//    println(res)
    res
  }
}

object ReplayRuntime {
  val trees = mutable.HashMap[Any,ArrayBuffer[Any]]()

  def addBinding(symbol: Any, x: Any): Unit = {
    trees.getOrElseUpdate(symbol, ArrayBuffer[Any]()) += x
  }

  def getBindings(symbol: Any): ArrayBuffer[Any] = {
    trees(symbol)
  }

  def bind(x: Any): Unit = macro ReplayRuntimeImpl.bindImpl
  def emit(x:Any)(bindings: Any): Any = macro ReplayRuntimeImpl.emitImpl

}
