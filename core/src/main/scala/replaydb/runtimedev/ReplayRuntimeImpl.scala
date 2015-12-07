package replaydb.runtimedev

import replaydb.event.Event
import replaydb.runtimedev.threadedImpl.{CoordinatorInterface, RunProgressCoordinator}

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

  def dataflowAnalysis(bindings: Iterable[c.Expr[Any]]): Array[Array[c.Expr[Any]]] = {
    trait Dataflow {
      val obj: Tree
    }
    case class ReadDataflow (obj: Tree) extends Dataflow
    case class WriteDataflow (obj: Tree) extends Dataflow
    case class BindingDesc (binding: c.Expr[Any], dataflow: List[Dataflow])

    class DependencyDetector {
      val df = ArrayBuffer[BindingDesc]()
      def add(x: c.Expr[Any]) = {
        val dataflow: List[Dataflow] = x.tree.collect {
          case Apply(Select(obj,meth: Name), _) if obj.tpe <:< typeOf[ReplayState] =>
            meth match {
              case TermName("get") | TermName("getRandom") =>
                ReadDataflow(obj)
              case TermName("merge") | TermName("add") =>
                WriteDataflow(obj)
              case _ =>
                throw new RuntimeException(s"unexpected method $meth")
            }
        }
        df += new BindingDesc(x, dataflow)
      }
      def analyze(): List[List[BindingDesc]] = {
        val writers = mutable.HashMap[String,ArrayBuffer[BindingDesc]]()
        for (bd <- df) {
          for (wdf <- bd.dataflow.filter(_.isInstanceOf[WriteDataflow])) {
            writers.getOrElseUpdate(wdf.obj.toString, ArrayBuffer[BindingDesc]()) += bd
          }
        }
        var dependencies: ArrayBuffer[(BindingDesc, mutable.HashSet[BindingDesc])] = df.map { bd =>
          (bd, mutable.HashSet[BindingDesc]() ++ bd.dataflow.filter(_.isInstanceOf[ReadDataflow]).flatMap(rdf => writers.getOrElse(rdf.obj.toString, List())))
        }
        val bindings = ArrayBuffer[List[BindingDesc]]()
        while (dependencies.nonEmpty) {
          val nodeps = dependencies.filter(_._2.isEmpty).map(_._1)
          if (nodeps.isEmpty) {
            throw new RuntimeException("cyclic dependencies")
          }
          bindings += nodeps.toList
          dependencies = dependencies.filter(_._2.nonEmpty)
          for (dep <- dependencies) {
            dep._2 --= nodeps
          }
        }
        bindings.toList
      }
    }

    val dd = new DependencyDetector
    for (binding <- bindings) {
      dd.add(binding)
    }
    dd.analyze().map(bl => bl.map(_.binding).toArray).toArray
  }

  def bindImpl(x: c.Expr[Any]): c.Expr[Unit] = {
    ReplayRuntime.addBinding(c.internal.enclosingOwner.owner, x)
    c.Expr[Unit](q"")
  }

  def emitImpl(bindings: Expr[Any]): c.Expr[RuntimeInterface] = {
    val bindings = ReplayRuntime.getBindings(c.internal.enclosingOwner.owner).
      asInstanceOf[ArrayBuffer[c.Expr[Any]]]
    val bindingsAnalyzed = dataflowAnalysis(bindings)
    val numPhases = bindingsAnalyzed.size

    var outstandingPrepares: mutable.Map[Type, ArrayBuffer[(TermName, Tree)]] = mutable.Map()

    val phaseCases = (for (i <- bindingsAnalyzed.size-1 to 0 by -1) yield {
      val m = mutable.HashMap[Type, ArrayBuffer[(TermName,Tree)]]()

      for (preps <- outstandingPrepares) preps match {
        case (tpe, body) => m.getOrElseUpdate(tpe, ArrayBuffer()) ++= body
      }
      outstandingPrepares = mutable.Map()

      for (b <- bindingsAnalyzed(i)) {
        b.tree match {
          case Function(params, body) =>
            if (params.size == 1 && params(0).tpt.tpe <:< typeOf[Event]) {

              body.foreach {
                case Apply(Select(obj,meth: Name), args: List[Tree]) if obj.tpe <:< typeOf[ReplayState] =>
                  meth match {
                    case TermName("get") | TermName("getRandom") =>
                      outstandingPrepares.getOrElseUpdate(params.head.tpt.tpe, ArrayBuffer()) += ((params.head.name,
                        Apply(Apply(Select(obj, TermName("getPrepare")), args), List(q"batchInfo"))))
                    case TermName("merge") | TermName("add") =>
                      // Nothing to be done
                    case _ =>
                      throw new RuntimeException(s"unexpected method $meth")
                  }
                case _ =>
              }

              m.getOrElseUpdate(params(0).tpt.tpe, ArrayBuffer()) += ((params(0).name, body))
            } else {
              throw new RuntimeException("unrecognized function with parameters " + params)
            }
          case _ => throw new RuntimeException("expected function")
        }
      }

      val cases = m.map{ case (tpt, trees) =>
        val statements = trees.map { case(termName, body) =>
          val rt = replaceTransform(termName, TermName("zz"))
          new Transformer {
            override def transform(tree: Tree): Tree = tree match {
              case st: SymTree if st.tpe != null && st.tpe <:< typeOf[BatchInfo]
                && st.symbol.name == TermName("defaultBatchInfo") =>
                q"batchInfo"
              case _ => super.transform(tree)
            }
          }.transform(rt.transform(body))
        }
        cq"zz : $tpt => ..$statements".asInstanceOf[CaseDef]
      }.toList ++ List(cq"_ => ".asInstanceOf[CaseDef])
      val me = Match(q"e", cases)
      cq"$i => $me".asInstanceOf[CaseDef]
    }).toList
    if (outstandingPrepares.nonEmpty) {
      throw new RuntimeException("Outstanding prepares remaining!")
    }
    val me = Match(q"phase", phaseCases)

    val ri =
      q"""
         new RuntimeInterface {
           import replaydb.event.Event
           def numPhases: Int = $numPhases
           def update(phase: Int, e: Event)(implicit batchInfo: BatchInfo): Unit = {
             $me
           }
         }
       """

    val res = c.Expr[RuntimeInterface](c.untypecheck(ri))
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
  def emit(bindings: Any): RuntimeInterface = macro ReplayRuntimeImpl.emitImpl

}
