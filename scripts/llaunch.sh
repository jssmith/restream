#!/bin/bash

port=$1
logfile="/home/ec2-user/log/run-$port.txt"
echo "logging to $logfile"
nohup /home/ec2-user/jdk1.8.0_66/bin/java -Xmx3000m -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.service.exec.WorkerService $port > logfile 2> $logfile.err < /dev/null &
