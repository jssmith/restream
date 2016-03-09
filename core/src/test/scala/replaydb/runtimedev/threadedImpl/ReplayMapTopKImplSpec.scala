package replaydb.runtimedev.threadedImpl

import org.scalatest.FlatSpec

class ReplayMapTopKImplSpec extends FlatSpec {

  case class Obj(a: Int) extends Ordered[Obj] {
    override def compare(that: Obj): Int = {
      a.compare(that.a)
    }
  }

  "A threaded ReplayMapTopK" should "return the top K elements" in {
    val rmtk = new ReplayMapTopKLazyImpl[Long, Obj](Obj(0))

    rmtk.merge(4, 1, _ => Obj(5))
    rmtk.merge(8, 1, _ => Obj(10))

    rmtk.merge(2, 2, _ => Obj(15))
    rmtk.merge(7, 2, _ => Obj(2))

    rmtk.merge(3, 3, _ => Obj(12))

    rmtk.merge(7, 4, _ => Obj(17))

    rmtk.merge(3, 5, _ => Obj(9))

    assert(rmtk.getTopK(5, 3) === List((2, Obj(15)), (3, Obj(12)), (5, Obj(9))))
  }

  "A threaded ReplayMapTopKProactive" should "return the top K elements" in {
    val rmtk = new ReplayMapTopKImpl[Long, Obj](Obj(0), 3)

    rmtk.merge(4, 1, _ => Obj(5))
    rmtk.merge(8, 1, _ => Obj(10))

    rmtk.merge(2, 2, _ => Obj(15))
    rmtk.merge(7, 2, _ => Obj(2))

    rmtk.merge(3, 3, _ => Obj(12))

    rmtk.merge(7, 4, _ => Obj(17))

    rmtk.merge(3, 5, _ => Obj(9))

    assert(rmtk.getTopK(5, 3) === List((2, Obj(15)), (3, Obj(12)), (5, Obj(9))))
  }

  "A threaded ReplayMapTopKProactive" should "return the top K elements 2" in {
    val rmtk = new ReplayMapTopKImpl[Long, Obj](Obj(0), 4)

    rmtk.merge(4, 1, _ => Obj(5))
    rmtk.merge(8, 1, _ => Obj(10))

    rmtk.merge(2, 2, _ => Obj(15))
    rmtk.merge(7, 2, _ => Obj(2))

    rmtk.merge(3, 3, _ => Obj(12))

    rmtk.merge(7, 4, _ => Obj(17))

    rmtk.merge(3, 5, _ => Obj(9))

    rmtk.merge(1, 6, _ => Obj(10))
    rmtk.merge(4, 6, _ => Obj(12))

    assert(rmtk.getTopK(5, 4) === List((2, Obj(15)), (3, Obj(12)), (6, Obj(12)), (5, Obj(9))))
  }

  "A threaded ReplayMapTopKProactive" should "return the top K elements even if some decrease" in {
    val rmtk = new ReplayMapTopKImpl[Long, Obj](Obj(0), 4)

    rmtk.merge(4, 1, _ => Obj(5))
    rmtk.merge(8, 1, _ => Obj(10))

    rmtk.merge(2, 2, _ => Obj(15))
    rmtk.merge(7, 2, _ => Obj(2))

    rmtk.merge(3, 3, _ => Obj(12))

    rmtk.merge(7, 4, _ => Obj(17))

    rmtk.merge(3, 5, _ => Obj(9))

    rmtk.merge(1, 6, _ => Obj(14))
    rmtk.merge(5, 6, _ => Obj(4))

    assert(rmtk.getTopK(5, 4) === List((2, Obj(15)), (3, Obj(12)), (5, Obj(9)), (1, Obj(5))))
  }

}
