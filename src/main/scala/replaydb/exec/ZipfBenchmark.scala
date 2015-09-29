package replaydb.exec

import org.apache.commons.math3.random.{RandomDataGenerator, MersenneTwister}
import replaydb.util.ProgressMeter

/**
 * Microbenchmark to test the speed of generating from a Zipfian distribution.
 * As the n increases we find that it slows down considerably, so this is
 * something that we may need to optimize.
 *
 * For alpha = 1 we find the following:
 *   n =  100 => 8771/s
 *   n = 1000 => 710/s
 *   n = 5000 => 116/s
 */
object ZipfBenchmark extends App {
  val n = args(0).toInt
  val alpha = args(1).toDouble
  val loops = args(2).toInt
  val pm = new ProgressMeter(1000)
  val rnd =  new RandomDataGenerator(new MersenneTwister(903485435L))
  for (i <- 0 until loops) {
    rnd.nextZipf(n, alpha)
    pm.increment()
  }
  pm.finished()
}
