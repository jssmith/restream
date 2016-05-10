package replaydb.exec.spam

import replaydb.event.MessageEvent
import replaydb.runtimedev.threadedImpl.Threaded
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.util.time._

class WordCountTopKStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {
  import replayStateFactory._

  val K = 20
  val WindowSize = 30.seconds

  val spamCounter: ReplayCounter = new ReplayCounter {
    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = { -1 }
    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = {}
    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = {}
  }

  val wordCounts: ReplayMapTopK[String, Int] = getReplayMapTopKLazy(0)

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

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { me: MessageEvent =>
      me.content.toLowerCase.split(" ").foreach(word => {
        wordCounts.merge(me.ts, word, _ + 1)
        wordCounts.merge(me.ts + WindowSize, word, _ - 1)
      })
    }
    bind { e: PrintSpamCounter =>
      println(s"TopKCounts (K = $K) are: ${wordCounts.getTopK(e.ts, K).mkString(", ")}")
    }
  }

}
