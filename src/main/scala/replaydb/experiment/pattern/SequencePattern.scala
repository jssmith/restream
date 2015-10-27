package replaydb.experiment.pattern

import replaydb.experiment.event.Event

/**
 * Created by erik on 10/27/15.
 */
class SequencePattern(p1: Pattern, p2: Pattern) extends Pattern {

  override def get_matches: Set[Event] = {
    // TODO this is clearly not correct
    var ret = Set[Event]()
    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
      if (mtch1.ts < mtch2.ts)
        ret += mtch1
    }
    ret
  }

  override def toString: String = {
    s"$p1 followed by $p2"
  }
}
