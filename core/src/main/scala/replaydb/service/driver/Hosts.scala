package replaydb.service.driver

import scala.io.Source

object Hosts {
  case class HostConfiguration(host: String, port: Int)
  def fromFile(fn: String): Array[HostConfiguration] = {
    Source.fromFile(fn).getLines().map(_.split(":")).map(x => new HostConfiguration(x(0), x(1).toInt)).toArray
  }
}