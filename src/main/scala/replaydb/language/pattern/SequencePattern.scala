package replaydb.language.pattern

import replaydb.language.event.Event

class SequencePattern(parent: Pattern) extends Pattern {
  type T = Seq[Event]

  var patterns = Seq(parent)

  def followed_by(p: SingleEventPattern): SequencePattern = {
    patterns :+= p
    this
  }

  override def get_matches: Set[Seq[Event]] = {
    // TODO this is clearly not correct
    var ret = Set[Seq[Event]]()
//    for (mtch1 <- p1.get_matches; mtch2 <- p2.get_matches) {
//      if (mtch1.ts < mtch2.ts)
//        ret += mtch1
//    }
    ret
  }

  override def toString: String = {
    patterns.mkString(" followed by ")
  }
}
