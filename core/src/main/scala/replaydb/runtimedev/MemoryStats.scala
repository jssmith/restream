package replaydb.runtimedev


object MemoryStats {

  def getStats(): String = {
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    val totalMemory = runtime.totalMemory() / mb
    val maxMemory = runtime.maxMemory() / mb
    val freeMemory = runtime.freeMemory() / mb
    val usedMemory = totalMemory - freeMemory
    s"""$usedMemory $freeMemory $totalMemory $maxMemory"""
  }
}
