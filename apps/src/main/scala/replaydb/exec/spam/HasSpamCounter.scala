package replaydb.exec.spam

import replaydb.runtimedev.ReplayCounter

trait HasSpamCounter {
  val spamCounter: ReplayCounter
}
