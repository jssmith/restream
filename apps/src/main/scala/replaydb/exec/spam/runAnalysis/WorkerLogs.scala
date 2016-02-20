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
    val allPerf = for (f <- perfFiles) yield {
      val perfText = Util.readFully(f)
      val readerThreadMs = findAllTimes(perfText, """reader thread time (\d+) ms""").sum
      val bossThreadMs = findAllTimes(perfText, """boss thread pool time: (\d+) ms""").sum
      val workerThreadMs = findAllTimes(perfText, """worker thread pool time: (\d+) ms""").sum
      val phaseThreadMs = findAllTimes(perfText, """thread \d+ \(phase \d+\) finished with elapsed cpu time (\d+) ms""").sum
      val kryoSendMs = findAllTimes(perfText, """sent_kryo_ms=(\d+)""").sum
      val kryoRecvMs = findAllTimes(perfText, """recv_kryo_ms=(\d+)""").sum
      val kryoSendBytes = findAllTimes(perfText, """sent_bytes=(\d+)""").sum
      val kryoRecvBytes = findAllTimes(perfText, """recv_bytes=(\d+)""").sum
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
    allPerf.reduce((p1: PerfSummary, p2:PerfSummary) => new PerfSummary(
      readerThreadMs = p1.readerThreadMs + p2.readerThreadMs,
      bossThreadMs = p1.bossThreadMs + p2.bossThreadMs,
      workerThreadMs = p1.workerThreadMs + p2.workerThreadMs,
      phaseThreadMs = p1.phaseThreadMs + p2.phaseThreadMs,
      kryoSendMs = p1.kryoSendMs + p2.kryoSendMs,
      kryoRecvMs = p1.kryoRecvMs + p2.kryoRecvMs,
      kryoSendBytes = p1.kryoSendBytes + p2.kryoSendBytes,
      kryoRecvBytes = p1.kryoRecvBytes + p2.kryoRecvBytes
    ))
  }
  def checkErrors(): Unit = {
    val errFiles = workerLogPath.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".err")
    })
    if (errFiles == null || errFiles.isEmpty) {
      throw new RuntimeException("expected size zero error files but found no error files")
    }
    errFiles.foreach { f =>
      if (f.length() != 0) {
        throw new RuntimeException(s"have errors in $f")
      }
    }
  }
}
