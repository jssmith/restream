package replaydb.exec.spam

import replaydb.event.{MessageEvent, NewFriendshipEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._



class WordCountStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {
  import replayStateFactory._

  val spamCounter: ReplayCounter = new ReplayCounter {
    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = {
      wordCounts.get(ts, keyword) match {
        case Some(x) => x
        case None => -1
      }
    }

    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {}

    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {}
  }

  val wordCounts: ReplayMap[String, Int] = getReplayMap(0)

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(wordCounts)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  val keyword = "finagle"

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { me: MessageEvent =>
      me.content.toLowerCase.split(" ").foreach(word => wordCounts.merge(me.ts, word, ct => ct + 1))
    }
    bind { e: PrintSpamCounter =>
      println(s"KEYWORD COUNT IS: ${wordCounts.get(ts = e.ts, key = keyword)}")
    }
  }
}
