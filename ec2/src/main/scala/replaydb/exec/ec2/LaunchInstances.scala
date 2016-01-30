package replaydb.exec.ec2

import javax.xml.bind.DatatypeConverter

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model._
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest


/**
 * Launch containers needed for ReplayDB testing
 */
object LaunchInstances extends App {

  if (args.length != 2) {
    println(
      """Usage: LaunchInstances numContainers prefix
      """.stripMargin)
    System.exit(1)
  }

  val numContainers = args(0).toInt
  val prefix = args(1)

  val initScript =
    """
      |#!/bin/bash
      |
      |yum update -y
      |#yum install -y git
      |
      |aws s3 cp s3://replaydb/jdk-8u72-linux-x64.gz /tmp
      |cd /opt
      |tar -zxf /tmp/jdk-8u72-linux-x64.gz
      |ln -s jdk1.8.0_72 java
      |rm /tmp/jdk-8u72-linux-x64.gz
      |
    """.stripMargin

  def toBase64(s: String) = {
    DatatypeConverter.printBase64Binary(s.getBytes("UTF-8"))
  }

  val credentials = new ProfileCredentialsProvider()

  val identityClient = new AmazonIdentityManagementClient(credentials)
  val ipr = identityClient.getInstanceProfile(new GetInstanceProfileRequest().withInstanceProfileName("replaydb-role"))
  val arn = ipr.getInstanceProfile.getArn

  val rir = new RunInstancesRequest()
  rir.
    withImageId("ami-d5ea86b5").
    withInstanceType("c4.large").
    withMinCount(numContainers).
    withMaxCount(numContainers).
    withKeyName("replaydb").
    withSecurityGroupIds("sg-4ed9072b").
    withEbsOptimized(true).
    withBlockDeviceMappings(
      new BlockDeviceMapping().
        withDeviceName("/dev/xvda").
        withEbs(new EbsBlockDevice().
          withIops(300).
          withVolumeSize(24).
          withDeleteOnTermination(true).
          withVolumeType("io1")
        )
    ).
    withUserData(toBase64(initScript)).
    withIamInstanceProfile(
      new IamInstanceProfileSpecification().
        withArn(arn)
    ).
    withPlacement(new Placement().withGroupName("replaydb-p1"))

  val ec2client = new AmazonEC2Client(credentials)
  ec2client.setEndpoint("ec2.us-west-1.amazonaws.com")
  val res = ec2client.runInstances(rir)

  val reservation = res.getReservation
  val instanceIds = {
    import scala.collection.JavaConversions._
    reservation.getInstances.map(i => i.getInstanceId)
  }

  var i = 0
  for (instanceId <- instanceIds) {
    val ctr = new CreateTagsRequest()
    ctr.
      withTags(new Tag("Name",s"$prefix-$i")).
      withResources(instanceId)
    ec2client.createTags(ctr)
    i += 1
  }

}
