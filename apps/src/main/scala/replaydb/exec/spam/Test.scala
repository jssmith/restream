package replaydb.exec.spam

import org.reflections.Reflections
import org.reflections.scanners.{SubTypesScanner, ResourcesScanner}
import replaydb.service.KryoCommandEncoder
import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/**
  * Created by erik on 12/10/15.
  */

object Test extends App {

  val t = new Test
  t.getAllClassesForSerialization()
  t.merge(5, t.dblStr)

  val term = typeOf[SpamDetectorStats].decl(TermName("getRuntimeInterface")).asTerm
  println(term)

}

class Test {

  val dblStr: Any=> Any = v => v.toString + v.toString

  def getAllClassesForSerialization(): Unit = {
    val reflections: Reflections = new Reflections("replaydb", new SubTypesScanner(false), new ResourcesScanner())
    val classes = reflections.getSubTypesOf(classOf[Object])
    val filtered = classes.filter(_.getName.contains("Test"))
    println("")
  }

  def merge(value: Any, fn: Any => Any): Any = {
    val enc = new KryoCommandEncoder(null)
    classOf[Test].getDeclaredFields.foreach(fld => enc.kryo.register(fld.getType))
    enc.kryo.register(fn.getClass)
    fn(value)
  }


}
