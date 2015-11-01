package replaydb.runtimedev.threadedImpl

import org.scalatest.FlatSpec
import replaydb.runtimedev.ReplayCounter

abstract class ReplayCounterImplBase[T <: ReplayCounter] extends FlatSpec {
  def getCounter: T
  def getName: String

  s"A ${getName}" should "count with one counter" in {
    val ctr = getCounter
    assert(ctr.get(ts = 100) === 0)
    ctr.add(value = 1, ts = 200)
    assert(ctr.get(ts = 100) === 0)
    assert(ctr.get(ts = 199) === 0)
    assert(ctr.get(ts = 200) === 1)
  }

  it should "count with adds at same time" in {
    val ctr = getCounter
    assert(ctr.get(ts = 100) === 0)
    ctr.add(value = 1, ts = 200)
    ctr.add(value = 6, ts = 200)
    assert(ctr.get(ts = 100) === 0)
    assert(ctr.get(ts = 199) === 0)
    assert(ctr.get(ts = 200) === 7)
    assert(ctr.get(ts = 300) === 7)
  }

  it should "count to a high number" in {
    val ctr = getCounter
    for (i <- 1 to 100) {
      ctr.add(value = i % 5, ts = i * 100)
    }
    assert(ctr.get(ts = 50) === 0)
    assert(ctr.get(ts = 150) === 1)
    assert(ctr.get(ts = 250) === 3)
    assert(ctr.get(ts = 399) === 6)
    assert(ctr.get(ts = 400) === 10)
    assert(ctr.get(ts = 599) === 10)
    assert(ctr.get(ts = 600) === 11)
    assert(ctr.get(ts = 9100) === 181)
    assert(ctr.get(ts = 9199) === 181)
    assert(ctr.get(ts = 9200) === 183)
  }

  it should "reject adds after gets" in {
    val ctr = getCounter
    ctr.add(ts = 100, value = 2)
    assert(ctr.get(ts = 150) === 2)
    ctr.add(ts = 200, value = 5)
    assert(ctr.get(ts = 160) === 2)
    try {
      ctr.add(ts = 155, value = 5)
      fail("expected exception on add after get")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    ctr.add(ts = 165, value = 7)
    assert(ctr.get(ts = 199) === 9)
    assert(ctr.get(ts = 200) === 14)
    assert(ctr.get(ts = 300) === 14)
    try {
      ctr.add(ts = 300, value = 5)
      fail("expected exception on add after get")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    assert(ctr.get(ts = 250) === 14)
    try {
      ctr.add(ts = 260, value = 5)
      fail("expected exception on add after get")
    } catch {
      case _: IllegalArgumentException => // expected
    }
  }

  it should "return counts in the past" in {
    val ctr = getCounter
    assert(ctr.get(ts = 10) === 0)
    ctr.add(ts = 50, value = 10)
    assert(ctr.get(ts = 49) === 0)
    assert(ctr.get(ts = 50) === 10)
    ctr.add(ts = 100, value = 1)
    ctr.add(ts = 150, value = 3)
    assert(ctr.get(ts = 150) === 14)
    assert(ctr.get(ts = 140) === 11)
  }
}
