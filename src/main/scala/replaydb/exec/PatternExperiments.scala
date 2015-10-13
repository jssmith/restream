package replaydb.exec

import java.io.{BufferedInputStream, FileInputStream}

import replaydb.event.{Event, MessageEvent, NewFriendshipEvent}
import replaydb.io.SocialNetworkStorage
import replaydb.util.ProgressMeter

import scala.collection.mutable.ArrayBuffer

/**
 * Experiments in pattern language
 */
object PatternExperiments extends App {
  val MS_DAY = 86400000

  trait Pattern {
  }

  val ANY = new Pattern() {
  }

  class EventPattern[T](val t: Class[T]) extends Pattern {
  }

  object EventPattern {
    def apply[T](t: Class[T]): EventPattern[T] = {
      new EventPattern(t)
    }

    def apply[T](t: Class[T], bindings: List[Binding]): EventPattern[T] = {
      ???
    }
  }

  case class NegatePattern(p: Pattern) extends Pattern {
  }

  case class PredicatePattern(p: Pattern, predicate: Expression) extends Pattern {
  }

  case class SequencePattern(x: List[Pattern]) extends Pattern {
  }

  case class ArbitraryRepeatPattern(p: Pattern) extends Pattern {
  }

  class Expression

  case class Binding(x: FieldReference, y: Expression)

  // TODO should we have an expression on the left?, or just make the whole thing an expression?
  case class FieldReference(name: String)
  val TimeReference = new FieldReference("ts")

  case class NamedVar(name: String) extends Expression

  case class TimeRange(begin: Expression, end: Expression) extends Expression

  // TODO this isn't really and expression, but want to pass it in that way
  case class LongValue(x: Long) extends Expression

  case class ArithmeticOp(opName: String, x: Expression, y: Expression) extends Expression

  case class BinaryLogicOp(opName: String, x: Expression, y: Expression) extends Expression

  trait State[T] extends Expression {
    def get: T
  }

  val UnitState = new State[Unit] {
    override def get: Unit = Unit
  }

  object Count {
    def apply[T](p: Pattern): State[Int] = {
      var ct = 0
      val s = new State[Int] {
        def get: Int = {
          ct
        }
      }
      register(p, () => {
        ct += 1
      })
      s
    }
  }

  // TODO more state variables
  // Also: - associative + commutative operators generally
  //       - associative but not commutative
  //       - distributions

  object Emit {
    def apply[T](p: Pattern, f: T => Unit): Unit = {
      //      register(p, () => {
      //        print("hit")
      //      })
      //      UnitState
    }
  }

  val patterns = new ArrayBuffer[(Pattern, () => Unit)]()

  def register[T](p: Pattern, cb: () => Unit): Unit = {
    patterns += p -> cb
  }

  def update(e: Event) = {
//    for ((p, cb) <- patterns) {
//      p.update(e) match {
//        case Some(x) => cb()
//        case None =>
//      }
//    }
  }

  // Counter of all messages
  val allMessageCount = Count(EventPattern(classOf[MessageEvent]))

  // Counter of all friend requests
  val allFriendRequestCount = Count(EventPattern(classOf[NewFriendshipEvent]))

  // Print all messages
  Emit(EventPattern(classOf[NewFriendshipEvent]), (e: Event) => println(e))

  // Print all messages sent by id 1
  Emit(EventPattern(classOf[MessageEvent], List(Binding(FieldReference("senderUserId"), LongValue(1)))), (e: Event) => println(e))

  // Count of response messages
  val responseCount = Count(
    SequencePattern(List(
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("A")),
          Binding(FieldReference("recipientUserId"), NamedVar("B"))
        )
      ),
      ArbitraryRepeatPattern(ANY)
      ,
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("B")),
          Binding(FieldReference("recipientUserId"), NamedVar("A"))
        )
      )
    ))
  )

  // Count of first response messages
  val firstResponseCount = Count(
    SequencePattern(List(
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("A")),
          Binding(FieldReference("recipientUserId"), NamedVar("B"))
        )
      ), ArbitraryRepeatPattern(NegatePattern(EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("B")),
          Binding(FieldReference("recipientUserId"), NamedVar("A"))
        )
      ))),
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("B")),
          Binding(FieldReference("recipientUserId"), NamedVar("A"))
        )
      )
    ))
  )

  // Count of first response messages
  val firstResponseCount2 = {
    val responseMessage = EventPattern(classOf[MessageEvent],
      List(
        Binding(FieldReference("senderUserId"), NamedVar("B")),
        Binding(FieldReference("recipientUserId"), NamedVar("A"))
      )
    )
    Count(
      SequencePattern(List(
        EventPattern(classOf[MessageEvent],
          List(
            Binding(FieldReference("senderUserId"), NamedVar("A")),
            Binding(FieldReference("recipientUserId"), NamedVar("B"))
          )
        ), ArbitraryRepeatPattern(NegatePattern(responseMessage)), responseMessage
      ))
    )
  }

  // Count of responses within 7 days
  val responseCountWeek = Count(
    SequencePattern(List(
    // TODO order top to bottom or bottom to top, what about conflicting order for timestamps
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("A")),
          Binding(FieldReference("recipientUserId"), NamedVar("B")),
          Binding(FieldReference("ts"), NamedVar("t"))
        )
      ),
      EventPattern(classOf[MessageEvent],
        List(
          Binding(FieldReference("senderUserId"), NamedVar("B")),
          Binding(FieldReference("recipientUserId"), NamedVar("A")),
          // TODO time ranges need to be first class entities
          Binding(TimeReference, TimeRange(ArithmeticOp("-",NamedVar("t"),LongValue(7*MS_DAY)),NamedVar("t")))
        )
      )
    ))
  )

  def printMatching[T](p: Pattern) = {
    Emit(p, (e: Event) => println(e))
  }

  // NewFriendship message sent count in last 7 days exceeds 5
  printMatching(PredicatePattern(
    EventPattern(classOf[NewFriendshipEvent],
      List(
        Binding(FieldReference("userIdA"), NamedVar("A")),
        Binding(FieldReference("ts"), NamedVar("t"))
      )
    ),ArithmeticOp("gt",Count(ArbitraryRepeatPattern(
      EventPattern(classOf[MessageEvent], List(
        Binding(FieldReference("senderUserId"), NamedVar("A")),
        Binding(FieldReference("ts"), TimeRange(ArithmeticOp("-",NamedVar("t"),LongValue(7*MS_DAY)),NamedVar("t")))
      ))
    )),LongValue(5))
  ))

  // NewFriendship when message sent count in last 7 days exceeds 5 or message received count in last 7 days exceeds 10
  printMatching(PredicatePattern(
    EventPattern(classOf[NewFriendshipEvent],
      List(
        Binding(FieldReference("userIdA"), NamedVar("A")),
        Binding(FieldReference("ts"), NamedVar("t"))
      )
    ),BinaryLogicOp("or",
      ArithmeticOp("gt",Count(EventPattern(classOf[MessageEvent], List(
        Binding(FieldReference("senderUserId"), NamedVar("A")),
        Binding(FieldReference("ts"), TimeRange(ArithmeticOp("-",NamedVar("t"),LongValue(7*MS_DAY)),NamedVar("t")))
      )
      )),LongValue(5)),
      ArithmeticOp("gt",Count(EventPattern(classOf[MessageEvent], List(
          Binding(FieldReference("recipientUserId"), NamedVar("A")),
          Binding(FieldReference("ts"), TimeRange(ArithmeticOp("-",NamedVar("t"),LongValue(7*MS_DAY)),NamedVar("t")))
        ))),LongValue(10)))
  ))

  // Add some Machine Learning


  val eventStorage = new SocialNetworkStorage
  val pm = new ProgressMeter(printInterval = 100000, extraInfo = () => s"${allMessageCount.get} ${allFriendRequestCount.get}")
  val r = eventStorage.readEvents(new BufferedInputStream(new FileInputStream("/tmp/events.out")), e => {
    update(e)
    pm.increment()
  })
  pm.finished()

}
