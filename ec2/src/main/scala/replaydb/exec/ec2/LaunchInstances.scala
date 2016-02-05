package replaydb.exec.ec2

import scala.collection.mutable

/**
 * Launch containers needed for ReplayDB testing - launches one master at
  * prefix-master (if not already present) and increases the number of workers at
  * prefix-worker-index to be at least numWorkers (if there are already >= numWorkers
  * launched, does nothing).
 */
object LaunchInstances extends App {

  if (args.length < 2) {
    println(
      """Usage: LaunchInstances numWorkers prefix [launchMaster = false]
      """.stripMargin)
    System.exit(1)
  }

  val numWorkers = args(0).toInt
  val prefix = args(1)
  val launchMaster = if (args.length > 2) args(2).toBoolean else false

  val initScript =
    """
      |#!/bin/bash
      |
      |yum update -y
      |
      |aws s3 cp s3://replaydb/jdk-8u72-linux-x64.gz /tmp
      |#wget -O /tmp/jdk-8u72-linux-x64.gz --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u72-b13/jdk-8u72-linux-x64.tar.gz
      |cd /opt
      |tar -zxf /tmp/jdk-8u72-linux-x64.gz
      |ln -s jdk1.8.0_72 java
      |rm /tmp/jdk-8u72-linux-x64.gz
      |update-alternatives --install /usr/bin/java java /opt/jdk1.8.0_72/jre/bin/java 20000
      |
      |yum install -y zsh emacs
      |wget -O /tmp/zsh.zip https://dl.dropboxusercontent.com/u/6350499/zsh.zip
      |unzip -d /home/ec2-user /tmp/zsh.zip
      |rm /tmp/zsh.zip
      |
      |# Add Erik's public ssh key
      |wget -O /tmp/replaydb.pub https://dl.dropboxusercontent.com/u/6350499/replaydb.pub
      |cat /tmp/replaydb.pub >> /home/ec2-user/.ssh/authorized_keys
      |rm /tmp/replaydb.pub
      |
    """.stripMargin
  val masterInitScript = initScript +
    """
      |
      |yum install -y git
      |git clone https://github.com/jssmith/replaydb /home/ec2-user/replaydb
      |chown -R ec2-user /home/ec2-user/replaydb
      |
      |curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
      |yum install sbt
      |
      |mkdir conf
    """.stripMargin
  val workerInitScript = initScript +
    """
      |mkdir replaydb-worker
      |mkdir log
    """.stripMargin

  val workerInstanceType = "c4.large"
  val masterInstanceType = "c4.large"
  val keyName = "replaydb"
  //val securityGroupId = "sg-97252df2" // Erik's account
  val securityGroupId = "sg-4ed9072b" // Johann
  val instanceProfileName = "replaydb-role"
  val placementGroupName = "replaydb-p1"
  val workerPrefix = prefix + "-worker"
  val masterName = prefix + "-master"

  if (launchMaster && Utils.getInstances(true, masterName).isEmpty) {
    println(s"Existing master instance not found; launching now at $masterName")
    Utils.launchInstances(masterInstanceType, keyName, securityGroupId, instanceProfileName, placementGroupName,
      List(masterName), masterInitScript)
  } else {
    println("Existing master instance found; not launching a new one.")
  }

  val workerNames = Utils.getInstances(true, workerPrefix).map(Utils.getName)
  val newWorkerNames = mutable.ArrayBuffer[String]()
  var idx = 0
  while (newWorkerNames.length + workerNames.length < numWorkers) {
    val newName = workerPrefix + s"-$idx"
    if (!workerNames.contains(newName)) {
      newWorkerNames += newName
    }
    idx += 1
  }

  if (newWorkerNames.nonEmpty) {
    println(s"Launching ${newWorkerNames.length} new instances at: ${newWorkerNames.mkString(", ")}")
    Utils.launchInstances(workerInstanceType, keyName, securityGroupId, instanceProfileName, placementGroupName,
      newWorkerNames.toList, workerInitScript)
  } else {
    println(s"Already found ${workerNames.length} worker instances; not launching any new ones.")
  }

}
