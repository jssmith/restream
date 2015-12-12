package replaydb.exec.spam

import replaydb.event.{Event}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._


/*
RULES:

1. spam if sent messages to non friends > 2 * (messages to friends) and total messages > 5

 */

class DoNothingSpamDetectorStats(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {
  import replayStateFactory._

  val spamCounter: ReplayCounter = getReplayCounter

  // TODO Ideally this becomes automated by the code generation portion
  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    val states = List(spamCounter)
    for (s <- states) {
      if (!s.isInstanceOf[ReplayState with Threaded]) {
        throw new UnsupportedOperationException
      }
    }
    states.asInstanceOf[Seq[ReplayState with Threaded]]
  }

  def getRuntimeInterface: RuntimeInterface = emit {
    bind { e: Event => }

    bind {
      e: PrintSpamCounter => println(s"\n\nSPAM COUNT is ${spamCounter.get(e.ts)}\n\n")
    }
  }
}
