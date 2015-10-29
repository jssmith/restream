package replaydb.language.pattern

import replaydb.language.{Match, Interval}
import replaydb.language.event.Event
import replaydb.language.rcollection.PatternRCollection
import scala.collection.immutable.Set

object Pattern {
  implicit def RCollectionFromPattern(p: Pattern): PatternRCollection = {
    new PatternRCollection(p)
  }

  implicit def SequencePatternFromPattern(p: Pattern): SequencePattern = {
    new SequencePattern(p)
  }

  def apply[T <: Event](e: T): SingleEventPattern[T] = {
    new SingleEventPattern[T](e)
  }

  def apply[T <: Event](e: T, es: T*): SingleEventPattern[T] = {
    new SingleEventPattern[T](e, es:_*)
  }
}

abstract class Pattern {

  def followed_by(p: SingleEventPattern[_]): SequencePattern = {
    val sp = new SequencePattern(this)
    sp followed_by p
  }

//  def with_interval(interval: Interval): IntervalPattern = {
//    new IntervalPattern(this, interval)
//  }

  def get_matches: Seq[Match[Event]]
}