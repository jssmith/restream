package replaydb.exec.ec2


/**
 * Show ip addresses of containers named with the given prefix
 */
object InstanceIps extends App {

  if (args.length < 1) {
    println(
      """Usage: InstanceIps prefix [privateOnly = false]
      """.stripMargin)
    System.exit(1)
  }

  val prefix = args(0)
  val privateOnly = if (args.length == 2) args(1).toBoolean else false

  for ( i <- Utils.getInstances(true, prefix)) {
    val publicIp = i.getPublicIpAddress
    val privateIp = i.getPrivateIpAddress
    println(s"$privateIp ${if (privateOnly) "" else publicIp}")
  }

}
