package replaydb.language.bindings

import replaydb.language.time.{TimeOffset, Timestamp}

import scala.util.Random

class NamedTimeIntervalBinding(val name: String, relativeTo: NamedBinding[Timestamp],
                               min: TimeOffset, max: TimeOffset)
  extends TimeIntervalBinding(relativeTo, min, max) with NamedBinding[Timestamp] {

  def this(name: String, ttb: TimeIntervalBinding) =
    this(name, ttb.relativeTo, ttb.min, ttb.max)

  // Used if we don't actually care about the name
  def this(ttb: TimeIntervalBinding) =
    this(NamedTimeIntervalBinding.getRandomString, ttb.relativeTo, ttb.min, ttb.max)

  override def toString: String = {
    super.toString + s" bound to $name"
  }
}

object NamedTimeIntervalBinding {
  // Return a pseudorandom string of printable characters
  // length 15
  def getRandomString: String = {
    val rand = new Random()
    (1 to 15).map(_ => rand.nextPrintableChar()).mkString("")
  }

}
