#!/bin/bash

port=$1
logfile="/home/ec2-user/log/$port"
echo "logging to $logfile"
nohup /home/ec2-user/jdk1.8.0_66/bin/java -Xmx3000m -Dreplaydb-logdir=/home/ec2-user/log -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.WorkerService $port > $logfile.launch-out 2> $logfile.launch-err < /dev/null &
