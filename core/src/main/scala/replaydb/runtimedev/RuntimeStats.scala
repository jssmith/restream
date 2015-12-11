package replaydb.runtimedev

import scala.collection.mutable.ArrayBuffer

abstract class RuntimeStats extends HasRuntimeInterface {

  val kryoTypes = ArrayBuffer[Class[_]]()

  def registerClass(cls: Class[_]): Unit = {
//    kryos.foreach(_.kryo.register(cls))
    kryoTypes += cls
  }

  def getKryoTypes: Array[Class[_]] = kryoTypes.toArray

}
