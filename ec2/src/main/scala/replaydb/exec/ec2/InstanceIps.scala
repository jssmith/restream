package replaydb.exec.ec2

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{DescribeInstancesResult, Instance}

import scala.collection.JavaConversions._

/**
 * Show ip addresses of containers named with the given prefix
 */
object InstanceIps extends App {

  if (args.length != 1) {
    println(
      """Usage: InstanceIps prefix
      """.stripMargin)
    System.exit(1)
  }

  val prefix = args(0)

  val credentials = new ProfileCredentialsProvider()
  val ec2client = new AmazonEC2Client(credentials)
  ec2client.setEndpoint("ec2.us-west-1.amazonaws.com")
  val instances: DescribeInstancesResult = ec2client.describeInstances()
  def getName(i: Instance): String = {
    i.getTags.find("Name" == _.getKey) match {
      case Some(tag) => tag.getValue
      case None => "No name"
    }
  }
  for (x <- instances.getReservations) {
    val instances = x.getInstances
    for (i <- instances) {
      val name = getName(i)
      if (name.startsWith(prefix) && i.getState.getName == "running") {
        val publicIp = i.getPublicIpAddress
        val privateIp = i.getPrivateIpAddress
        println(s"$privateIp $publicIp")
      }
    }
  }
}
