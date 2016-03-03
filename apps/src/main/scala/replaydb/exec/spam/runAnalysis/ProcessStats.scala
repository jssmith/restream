package replaydb.exec.spam.runAnalysis

import java.io._

import scala.io.Source

object ProcessStats extends App {
  if (args.length != 1) {
    println("Usage: ProcessStats statsDirectory")
    System.exit(1)
  }

  val statsDirectory = new File(args(0))
  if (!statsDirectory.exists() || !statsDirectory.isDirectory) {
    throw new RuntimeException(s"Not a directory: $statsDirectory")
  }
  System.out.println(s"processing stats for $statsDirectory")

  val timingFiles = statsDirectory.listFiles(new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = {
      name.endsWith(".timing")
    }
  })

  System.out.println(s"number of timing files is ${timingFiles.length}")

  def getTime(timingFile: File): Long = {
    val realTimeRe = """real\s+(\d+)m(\d+).(\d+)s""".r
    val txt = Source.fromFile(timingFile).getLines().mkString("\n")
    val m = realTimeRe.findFirstMatchIn(txt)
    m match {
      case Some(m) =>
        m.group(1).toInt * 60000 + m.group(2).toInt * 1000 + m.group(3).toInt
      case None =>
        throw new RuntimeException(s"unexpected - no match on time in $timingFile")
    }
  }

  def getRunconfig(timingFile: File): LoggedRunConfiguration = {
    val rcRe = """(\d+)-(\d+)-(\d+)-([a-zA-Z\.]+)-(default|parallel|g1|cms).timing""".r
    val name = timingFile.getName
    name match {
      case rcRe(numHosts, numPartitions, iteration, detector, gc) =>
        new LoggedRunConfiguration(name, numHosts.toInt, numPartitions.toInt,
          iteration.toInt, detector)
    }
  }

  val outputFile = new File(statsDirectory, "performance.csv")
  val pw = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))
  pw.print("detector,hosts,partitions,overall_ms,")
  pw.println("reader_thread_ms,phase_threads_ms,io_boss_ms,io_worker_ms,kryo_send_ms,kryo_recv_ms,kryo_send_bytes,kryo_recv_bytes,kryo_send_skew,kryo_recv_skew")
  try {
    for (tf <- timingFiles) {
      val rc = getRunconfig(tf)
      try {
        val completionMs = getTime(tf)
        //    println(s"have timing file $tf ($rc) with time ${getTime(tf)}")
        val wl = new WorkerLogs(new File(statsDirectory,  rc.getLogName), rc)
        wl.checkErrors()
        pw.println(s"${rc.getCSV},$completionMs,${wl.summarizePerf().getCsv}")
      } catch {
        case e: RuntimeException =>
          println(s"skipping $rc ${e.getMessage()}")
      }
    }
  } finally {
    pw.close()
  }
}
