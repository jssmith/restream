package replaydb.runtimedev.threadedImpl

import org.scalatest.FlatSpec

class ReplayMapImplSpec extends FlatSpec {
  "A ReplayMap2Impl" should "implement some counters on a single key" in {
    val m = new ReplayMapImpl[Int, Int](0)
    assert(m.get(ts = 10, key = 1) === None)
    m.update(ts = 50, key = 1, _ => 10)
    assert(m.get(ts = 49, key = 1) === None)
    assert(m.get(ts = 50, key = 1) === Some(10))
    m.update(ts = 100, key = 1, _ + 1)
    m.update(ts = 150, key = 1, _ + 3)
    assert(m.get(ts = 150, key = 1) === Some(14))
    assert(m.get(ts = 140, key = 1) === Some(11))
  }

  it should "implement some counters on multiple keys" in {
    val m = new ReplayMapImpl[Int, Int](5)
    m.update(ts = 50, key = 1, _ + 7)
    m.update(ts = 50, key = 2, _ * 3)
    m.update(ts = 100, key = 1, _ * 3)
    m.update(ts = 100, key = 2, _ + 7)
    assert(m.get(ts = 200, key = 2) === Some(22))
    assert(m.get(ts = 100, key = 1) === Some(36))
    m.update(ts = 75, key = 3, _ + 12)
    assert(m.get(ts = 60, key = 3) === None)
    assert(m.get(ts = 100, key = 3) === Some(17))
  }
}
