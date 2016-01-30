package replaydb.exec.ec2

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.DescribeInstancesResult

import scala.collection.JavaConversions._

/**
 * Show basic information about the instances
 */
object ShowInstances extends App {
  val credentials = new ProfileCredentialsProvider()
  val ec2client = new AmazonEC2Client(credentials)
  ec2client.setEndpoint("ec2.us-west-1.amazonaws.com")
  val instances: DescribeInstancesResult = ec2client.describeInstances()
  for (x <- instances.getReservations) {
    val instances = x.getInstances
    for (i <- instances) {
      println(i.getInstanceId)
      println(i.getImageId)
      println(i.getHypervisor)
      println(i.getVirtualizationType)
      println(i.getKeyName)
      val name = i.getTags.find("Name" == _.getKey) match {
        case Some(tag) => tag.getValue
        case None => "No name"
      }
      println(name)
      println(i.getInstanceType)
      println(i.getSecurityGroups)
      println(i.getEbsOptimized)
      println(i.getState)
      println("----------------")
    }
  }
}
