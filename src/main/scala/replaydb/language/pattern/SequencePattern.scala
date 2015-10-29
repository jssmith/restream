package replaydb.language.pattern

import replaydb.language.Match
import replaydb.language.event.Event

class SequencePattern(parent: Pattern) extends Pattern {

  var patterns = Seq(parent)

  override def followed_by(p: SingleEventPattern[_]): SequencePattern = {
    patterns :+= p
    this
  }

  override def get_matches: Seq[Match[Event]] = {
    // TODO this is clearly not correct
//    var ret = Seq[Match[Event]]()
//    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
//      if (mtch1.ts < mtch2.ts)
//        ret += mtch1
//    }
//    ret
    Seq()
  }

  override def toString: String = {
    patterns.mkString(" followed by ")
  }
}
