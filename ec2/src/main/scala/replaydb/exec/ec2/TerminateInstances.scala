package replaydb.exec.ec2

import scala.collection.JavaConversions._
import com.amazonaws.services.ec2.model.TerminateInstancesRequest

/**
  * Terminate enough worker instances (at prefix-worker-index) to bring the
  * total number down to numWorkers. Also, if terminateMaster is true,
  * terminate the master (at prefix-master).
  */
object TerminateInstances extends App {
  if (args.length < 2) {
    println(
      """Usage: TerminateInstances numWorkers prefix [terminateMaster = false]
      """.stripMargin)
    System.exit(1)
  }

  val numWorkers = args(0).toInt
  val prefix = args(1)
  val workerPrefix = prefix + "-worker"
  val masterName = prefix + "-master"
  val terminateMaster = if (args.length > 2) args(2).toBoolean else false

  val (ec2client, _) = Utils.getEC2ClientAndCredentials

  var masterTerminationList = List[String]()
  if (terminateMaster) {
    val master = Utils.getInstances(true, masterName)
    if (master.nonEmpty) {
      // Should only be one but just go ahead and terminate anything matching
      masterTerminationList = master.map(_.getInstanceId)
      println(s"Terminating master at instance ID: $masterTerminationList")
    }
  }

  val workers = Utils.getInstances(true, workerPrefix).sortBy(Utils.getName).drop(numWorkers)
  val workerIDs = workers.map(_.getInstanceId)
  if (workers.nonEmpty) {
    println(s"Terminating worker instances with names (${workers.map(Utils.getName).mkString(", ")}) and IDs (${workerIDs.mkString(", ")})")
  }

  if ((workerIDs ++ masterTerminationList).nonEmpty) {
    ec2client.terminateInstances(new TerminateInstancesRequest(workerIDs ++ masterTerminationList))
  }

}
