package replaydb.language.pattern

import replaydb.language.Match
import replaydb.language.event.Event
import replaydb.language.rcollection.PatternRCollection
import scala.collection.immutable.Set

object Pattern {
  implicit def RCollectionFromPattern[T <: Event](p: Pattern[T]): PatternRCollection[T] = {
    new PatternRCollection[T](p)
  }

  // TODO is this actually used? I Think no
//  implicit def SequencePatternFromPattern[T <: Event](p: SingleEventPattern[T]): HomogeneousSequencePattern[T] = {
//    new HomogeneousSequencePattern[T](p)
//  }

  def apply[T <: Event](e: T): SingleEventPattern[T] = {
    new SingleEventPattern[T](e)
  }

//  def apply[T <: Event](e: T, es: T*): SingleEventPattern[T] = {
//    new SingleEventPattern[T](e, es:_*)
//  }
}

abstract class Pattern[+T <: Event] {

//  def with_interval(interval: Interval): IntervalPattern = {
//    new IntervalPattern(this, interval)
//  }

  def getMatches: Seq[Match[T]]
}