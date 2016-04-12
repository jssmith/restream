package replaydb.exec.ec2

import javax.xml.bind.DatatypeConverter

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model._
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient
import com.amazonaws.services.identitymanagement.model.GetInstanceProfileRequest

import scala.collection.JavaConversions._

object Utils {

  val regionUrl = "ec2.us-west-1.amazonaws.com"

  def toBase64(s: String) = {
    DatatypeConverter.printBase64Binary(s.getBytes("UTF-8"))
  }

  def getName(i: Instance): String = {
    i.getTags.find("Name" == _.getKey) match {
      case Some(tag) => tag.getValue
      case None => "No name"
    }
  }

  def getEC2ClientAndCredentials: (AmazonEC2Client, ProfileCredentialsProvider) = {
    val credentials = new ProfileCredentialsProvider()
    val ec2client = new AmazonEC2Client(credentials)
    ec2client.setEndpoint(Utils.regionUrl)
    (ec2client, credentials)
  }

  // Return all instances matching a given prefix, or if none is provided,
  // return all instances. If runningOnly is true, only return running instances.
  def getInstances(runningOnly: Boolean = false, prefix: String = null): List[Instance] = {
    val (ec2client, _) = getEC2ClientAndCredentials
    val instances: DescribeInstancesResult = ec2client.describeInstances()
    instances.getReservations.flatMap(_.getInstances
      .filter(i => if (prefix == null) true else getName(i).startsWith(prefix))
      .filter(i => if (runningOnly) i.getState.getCode == 16 else true)).toList
  }


  def launchInstances(instanceType: String, keyName: String, securityGroup: String,
                      instanceProfileName: String, placementGroupName: String, names: List[String],
                      initScript: String): Unit = {

    val (ec2client, credentials) = getEC2ClientAndCredentials
    val identityClient = new AmazonIdentityManagementClient(credentials)
    val ipr = identityClient.getInstanceProfile(new GetInstanceProfileRequest().withInstanceProfileName(instanceProfileName))
    val arn = ipr.getInstanceProfile.getArn

    val rir = new RunInstancesRequest()
    rir.
      //withImageId("ami-d5ea86b5").  // Normal Amazon Linux
      withImageId("ami-5599ef35").  // Image with 2 SSD volumes attached
      withInstanceType(instanceType).
      withMinCount(names.length).
      withMaxCount(names.length).
      withKeyName(keyName).
      withSecurityGroups(securityGroup).
      // EBS-optimized and larger EBS size not necessary since data doesn't live there anymore
      //      withEbsOptimized(true).
      //      withBlockDeviceMappings(
      //      new BlockDeviceMapping().
      //        withDeviceName("/dev/xvda").
      //        withEbs(new EbsBlockDevice().
      //          withIops(300).
      //          withVolumeSize(24).
      //          withDeleteOnTermination(true).
      //          withVolumeType("io1")
      //        ),
      //    ).
      withUserData(toBase64(initScript)).
      withIamInstanceProfile(
        new IamInstanceProfileSpecification().
          withArn(arn)
      ).
      withPlacement(new Placement().withGroupName(placementGroupName))

    val res = ec2client.runInstances(rir)

    val reservation = res.getReservation
    val instanceIds = {
      import scala.collection.JavaConversions._
      reservation.getInstances.map(i => i.getInstanceId)
    }

    setInstanceNames(ec2client, instanceIds.toList, names)
  }

  def launchSpotInstances(instanceType: String, keyName: String, securityGroup: String,
                          instanceProfileName: String, placementGroupName: String, names: List[String],
                          initScript: String, spotBid: Float): Unit = {

    val (ec2client, credentials) = getEC2ClientAndCredentials
    val identityClient = new AmazonIdentityManagementClient(credentials)
    val ipr = identityClient.getInstanceProfile(new GetInstanceProfileRequest().withInstanceProfileName(instanceProfileName))
    val arn = ipr.getInstanceProfile.getArn

    val rsir = new RequestSpotInstancesRequest()
    rsir.withSpotPrice(spotBid.toString).withInstanceCount(names.length)

    val launchSpec = new LaunchSpecification()
      //withImageId("ami-d5ea86b5").  // Normal Amazon Linux
      launchSpec.withImageId("ami-5599ef35").  // Image with 2 SSD volumes attached
      withInstanceType(instanceType).
      withKeyName(keyName).
      withSecurityGroups(securityGroup).
      withUserData(toBase64(initScript)).
      withIamInstanceProfile(
        new IamInstanceProfileSpecification().
          withArn(arn)
      ).
    withPlacement(new SpotPlacement().withGroupName(placementGroupName))

    rsir.setLaunchSpecification(launchSpec)

    val res = ec2client.requestSpotInstances(rsir)
    val requestResponses = res.getSpotInstanceRequests

    requestResponses.foreach(rr => println(s"Created spot request: ${rr.getSpotInstanceRequestId}"))
    // Setup an arraylist to collect all of the request ids we want to
    // watch hit the running state.
    val spotInstanceRequestIds = requestResponses.map(_.getSpotInstanceRequestId)

    val instanceIds = waitForSpotRequestsToFulfill(ec2client, spotInstanceRequestIds.toList)

    setInstanceNames(ec2client, instanceIds, names)
  }

  def setInstanceNames(ec2client: AmazonEC2Client, instanceIds: List[String], names: List[String]): Unit = {
    var i = 0
    for (instanceId <- instanceIds) {
      val ctr = new CreateTagsRequest()
      ctr.
        withTags(new Tag("Name", names(i))).
        withResources(instanceId)
      ec2client.createTags(ctr)
      i += 1
    }
  }

  // wait for all request IDs to no longer be open, return their associated instance IDs
  def waitForSpotRequestsToFulfill(ec2client: AmazonEC2Client, requestIDs: List[String]): List[String] = {
    var instanceIDs: Option[List[String]] = None

    do {
      // Create the describeRequest object with all of the request ids
      // to monitor (e.g. that we started).
      val describeRequest = new DescribeSpotInstanceRequestsRequest()
      describeRequest.setSpotInstanceRequestIds(requestIDs)

      instanceIDs = try {
        // Retrieve all of the requests we want to monitor.
        val describeResult = ec2client.describeSpotInstanceRequests(describeRequest)
        val describeResponses = describeResult.getSpotInstanceRequests

        // Look through each request and determine if they are all in
        // the active state.
        if (!describeResponses.exists(_.getState == "open")) {
          Some(describeResponses.map(_.getInstanceId).toList)
        } else {
          None
        }
      } catch {
        // If we have an exception, ensure we don't break out of
        // the loop. This prevents the scenario where there was
        // blip on the wire
        case e: AmazonServiceException => None
      }

      try {
        // Sleep for 20 seconds.
        Thread.sleep(20*1000)
      } catch {
        case e: Exception => // Do nothing because it woke up early.
      }
    } while (instanceIDs.isEmpty)
    instanceIDs.get
  }
}
