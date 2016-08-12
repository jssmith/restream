package replaydb.exec.ec2


/**
 * Show ip addresses of containers named with the given prefix
 */
object InstanceIps extends App {

  if (args.length < 3) {
    println(
      """Usage: InstanceIps profile region prefix [privateOnly = false]
      """.stripMargin)
    System.exit(1)
  }

  val profile = args(0)
  val region = args(1)
  val prefix = args(2)
  val privateOnly = if (args.length == 2) args(1).toBoolean else false

  val utils = new Utils(profile, region)
  for ( i <- utils.getInstances(true, prefix)) {
    val publicIp = i.getPublicIpAddress
    val privateIp = i.getPrivateIpAddress
    println(s"$privateIp ${if (privateOnly) "" else publicIp}")
  }

}
