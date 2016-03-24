package replaydb.exec.spam

import replaydb.event.{Event, MessageEvent}
import replaydb.runtimedev.ReplayRuntime._
import replaydb.runtimedev._
import replaydb.runtimedev.threadedImpl._


class DoNothingSpamDetectorStats2(replayStateFactory: replaydb.runtimedev.ReplayStateFactory) extends HasRuntimeInterface
  with HasSpamCounter with HasReplayStates[ReplayState with Threaded] {

  val spamCounter: ReplayCounter = new ReplayCounter {
    override def get(ts: Long)(implicit batchInfo: BatchInfo): Long = 0L
    override def getPrepare(ts: Long)(implicit batchInfo: BatchInfo): Unit = { }
    override def add(value: Long, ts: Long)(implicit batchInfo: BatchInfo): Unit = { }
  }

  def getAllReplayStates: Seq[ReplayState with Threaded] = {
    Nil
  }

  def getRuntimeInterface: RuntimeInterface = new RuntimeInterface {
    override def update(phase: Int, e: Event)(implicit batchInfo: BatchInfo): Unit = { }
    override def numPhases: Int = 1
  }
}
