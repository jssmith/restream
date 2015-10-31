//package replaydb.language.pattern
//
//import replaydb.language.{Match, Interval}
//import replaydb.language.event.Event
//import scala.collection.immutable.Set
//
//class IntervalPattern(basePattern: Pattern, interval: Interval) extends Pattern {
//
//  override def toString: String = {
//    basePattern.toString + s", from $interval"
//  }
//
//  override def get_matches: Set[Match[Event]] = {
//    Set() // not yet implemented...
//  }
//}
