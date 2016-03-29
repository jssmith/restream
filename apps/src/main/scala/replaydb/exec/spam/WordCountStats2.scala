package replaydb.exec.spam

import replaydb.event.{MessageEvent, Event}
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._


class WordCountStats2(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {

  import replayStateFactory._

  val spamCounter: ReplayCounter = new ReplayCounter {
    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = 0L

    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {}

    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {}
  }

  val wordCounts: ReplayMap[String, Int] = getReplayMap(0)

  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(wordCounts)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  def getRuntimeInterface: RuntimeInterface = new RuntimeInterface {
    val keyword = "finagle"
    override def update(phase: Int, e: Event)(implicit batchInfo: BatchInfo): Unit = {
      phase match {
        case 0 =>
          e match {
            case me: MessageEvent =>
              me.content.toLowerCase.split(" ").foreach(word => wordCounts.merge(me.ts, word, ct => ct + 1))
            case e: PrintSpamCounter =>
              wordCounts.getPrepare(e.ts, keyword)
            case _ =>
          }
        case 1 =>
          e match {
            case e: PrintSpamCounter =>
              println(s"KEYWORD COUNT IS: ${wordCounts.get(ts = e.ts, key = keyword)}")
            case _ =>
          }
      }
    }

    override def numPhases: Int = 2

  }
}
