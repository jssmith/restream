#!/bin/bash
#
# Local launch. Invoked by dlaunch.sh
# Usage: ./llaunch.sh port_number memory_size enable_debug
#

port=$1
mem_size=$2
logfile="/home/ec2-user/log/$port"
echo "logging to $logfile"

JAVA_DEBUG_OPTS=""
if [[ $3 = "true" ]]; then
  source /home/ec2-user/replaydb-worker/setup_java_debug_opts.sh
fi

nohup java $JAVA_DEBUG_OPTS -Xmx$mem_size -Dreplaydb-logdir=/home/ec2-user/log -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.WorkerService $port > $logfile.launch-out 2> $logfile.launch-err < /dev/null &
