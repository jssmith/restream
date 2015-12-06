package replaydb.util

class MemoryStats(totalMemory: Long, maxMemory: Long, freeMemory: Long) {
  val bytesPerMb = 1024 * 1024
  override def toString(): String = {
    s"max: ${maxMemory/bytesPerMb}, free: ${freeMemory/bytesPerMb}"
  }
}

object MemoryStats {
  def getMemoryStats(): MemoryStats = {
    val runtime = Runtime.getRuntime
    new MemoryStats(runtime.totalMemory(), runtime.maxMemory(),
      runtime.freeMemory())
  }
}
