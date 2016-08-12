package replaydb.exec.ec2

import scala.collection.JavaConversions._
import com.amazonaws.services.ec2.model.TerminateInstancesRequest

/**
  * Terminate enough worker instances (at prefix-worker-index) to bring the
  * total number down to numWorkers. Also, if terminateMaster is true,
  * terminate the master (at prefix-master).
  */
object TerminateInstances extends App {
  if (args.length < 4) {
    println(
      """Usage: TerminateInstances profile region numWorkers prefix [terminateMaster = false]
      """.stripMargin)
    System.exit(1)
  }

  val profile = args(0)
  val region = args(1)
  val numWorkers = args(2).toInt
  val prefix = args(3)
  val workerPrefix = prefix + "-worker"
  val masterName = prefix + "-master"
  val terminateMaster = if (args.length > 2) args(2).toBoolean else false

  val utils = new Utils(profile, region)
  val (ec2client, _) = utils.getEC2ClientAndCredentials

  var masterTerminationList = List[String]()
  if (terminateMaster) {
    val master = utils.getInstances(true, masterName)
    if (master.nonEmpty) {
      // Should only be one but just go ahead and terminate anything matching
      masterTerminationList = master.map(_.getInstanceId)
      println(s"Terminating master at instance ID: $masterTerminationList")
    }
  }

  val workers = utils.getInstances(true, workerPrefix).sortBy(utils.getName).drop(numWorkers)
  val workerIDs = workers.map(_.getInstanceId)
  if (workers.nonEmpty) {
    println(s"Terminating worker instances with names (${workers.map(utils.getName).mkString(", ")}) and IDs (${workerIDs.mkString(", ")})")
  }

  if ((workerIDs ++ masterTerminationList).nonEmpty) {
    ec2client.terminateInstances(new TerminateInstancesRequest(workerIDs ++ masterTerminationList))
  }

}
