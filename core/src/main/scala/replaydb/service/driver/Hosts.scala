package replaydb.service.driver

import scala.io.Source

object Hosts {
  case class HostConfiguration(host: String, port: Int)

  /**
   * Reads the configuration from a text file.
   * Format is one line per worker, as ip address:port
   *
   * @param fn host configuration file name
   * @return host configuration
   */
  def fromFile(fn: String): Array[HostConfiguration] = {
     Source.fromFile(fn).getLines().filter(_ != "").map(_.split(":")).map(x => new HostConfiguration(x(0), x(1).toInt)).toArray
  }
}