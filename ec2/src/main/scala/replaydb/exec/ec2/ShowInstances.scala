package replaydb.exec.ec2

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesResult
import replaydb.exec.ec2.InstanceIps._

import scala.collection.JavaConversions._

/**
 * Show basic information about the instances
 */
object ShowInstances extends App {
  if (args.length < 2) {
    println(
      """Usage: ShowInstances profile region [ showRunningOnly ]
      """.stripMargin)
    System.exit(1)
  }

  val profile = args(0)
  val region = args(1)
  val showRunningOnly = args.length > 2 && args(2) == "true"
  val utils = new Utils(profile, region)
  val (ec2client, _) = utils.getEC2ClientAndCredentials
  val instances: DescribeInstancesResult = ec2client.describeInstances()
  for (x <- instances.getReservations) {
    val instances = x.getInstances
    for (i <- instances if !showRunningOnly || i.getState.getCode == 16) {
      println(s"Instance / Image ID                  : ${i.getInstanceId} // ${i.getImageId}")
      println(s"Hypervisor / Virt. Type / Inst. Type : ${i.getHypervisor} // ${i.getVirtualizationType} // ${i.getInstanceType}")
      println(s"Key / Security Group                 : ${i.getKeyName} // ${i.getSecurityGroups}")
      val name = i.getTags.find("Name" == _.getKey) match {
        case Some(tag) => tag.getValue
        case None => "No name"
      }
      println(s"Name                                 : $name")
      println(s"State                                : ${i.getState}")
      println(s"Public / Private IP                  : ${i.getPublicIpAddress} / ${i.getPrivateIpAddress}")
      println("----------------")
    }
  }
  println(s"Total of ${instances.getReservations.map(_.getInstances.length).sum} instances, " +
    s"of which ${instances.getReservations.map(_.getInstances.count(_.getState.getCode == 16)).sum} are running")
}
