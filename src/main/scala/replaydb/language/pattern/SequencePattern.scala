package replaydb.language.pattern

import replaydb.language.Match
import replaydb.language.event.Event
import replaydb.language.time.Interval

class SequencePattern(parent: Pattern) extends Pattern {

  var patterns = Seq(parent)
  // intervals[i] is the allowable time interval between patterns[i] and patterns[i-1] 
  var intervals: Seq[Interval] = Seq()

  override def followedBy(p: SingleEventPattern[_]): SequencePatternWithoutInterval = {
    patterns :+= p
    new SequencePatternWithoutInterval(this)
  }

  override def getMatches: Seq[Match[Event]] = {
    // TODO this is clearly not correct
//    var ret = Seq[Match[Event]]()
//    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
//      if (mtch1.ts < mtch2.ts)
//        ret += mtch1
//    }
//    ret
    Seq()
  }
  
  def setNextInterval(interval: Interval): Unit = {
    if (intervals.size == patterns.size) {
      throw new RuntimeException("Can't have an interval without a corresponding pattern!")
    }
    intervals :+= interval
  }

  override def toString: String = {
    patterns.head.toString +
      (for ((p, i) <- patterns.tail zip intervals)
      yield s" followed by $p within $i").mkString("")
  }
}

