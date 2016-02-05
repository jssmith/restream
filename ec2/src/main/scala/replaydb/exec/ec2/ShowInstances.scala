package replaydb.exec.ec2

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesResult

import scala.collection.JavaConversions._

/**
 * Show basic information about the instances
 */
object ShowInstances extends App {
  val showRunningOnly = args.length > 0 && args(0) == "true"
  val credentials = new ProfileCredentialsProvider()
  val ec2client = new AmazonEC2Client(credentials)
  ec2client.setEndpoint(Utils.regionUrl)
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
