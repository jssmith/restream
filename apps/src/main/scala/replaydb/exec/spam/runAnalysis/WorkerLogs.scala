package replaydb.exec.spam.runAnalysis

import java.io.{FilenameFilter, File}

class WorkerLogs(workerLogPath: File, runConfig: LoggedRunConfiguration) {
  private def findFiles(pattern: String): Array[File] = {
    workerLogPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        val errFile = pattern.r
        name match {
          case errFile(x) => true
          case _ =>
            println(s"no match on $name")
            false
        }
      }
    })
  }
  private def findAllTimes(txt: String, pattern: String): Array[Long] = {
    val regEx = pattern.r
    (for (m <- regEx.findAllMatchIn(txt)) yield {
      m.group(1).toLong
    }).toArray
  }
  def summarizePerf(): PerfSummary = {
    val perfFiles = workerLogPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".perf")
    })
    val allContent = (for (f <- perfFiles) yield {
      Util.readFully(f)
    }).mkString("\n")
    val readerThreadMs = findAllTimes(allContent, """reader thread time (\d+) ms""").sum
    val bossThreadMs = findAllTimes(allContent, """boss thread pool time: (\d+) ms""").sum
    val workerThreadMs = findAllTimes(allContent, """worker thread pool time: (\d+) ms""").sum
    val phaseThreadMs = findAllTimes(allContent, """thread \d+ \(phase \d+\) finished with elapsed cpu time (\d+) ms""").sum
    val kryoSendMs = findAllTimes(allContent, """sent_kryo_ms=(\d+)""").sum
    val kryoRecvMs = findAllTimes(allContent, """recv_kryo_ms=(\d+)""").sum
    val kryoSendBytes = findAllTimes(allContent, """sent_bytes=(\d+)""").sum
    val kryoRecvBytes = findAllTimes(allContent, """recv_bytes=(\d+)""").sum
    new PerfSummary(
      readerThreadMs = readerThreadMs,
      bossThreadMs = bossThreadMs,
      workerThreadMs = workerThreadMs,
      phaseThreadMs = phaseThreadMs,
      kryoSendMs = kryoSendMs,
      kryoRecvMs = kryoRecvMs,
      kryoSendBytes = kryoSendBytes,
      kryoRecvBytes = kryoRecvBytes
    )
  }
  def checkErrors(): Unit = {
    val errFiles = workerLogPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".err")
    })
    if (errFiles.isEmpty) {
      throw new RuntimeException("expected size zero error files but found no error files")
    }
    errFiles.foreach { f =>
      if (f.length() != 0) {
        throw new RuntimeException(s"have errors in $f")
      }
    }
  }
}
