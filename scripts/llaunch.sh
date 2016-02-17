#!/bin/bash
#
# Local launch. Invoked by dlaunch.sh
# Usage: ./llaunch.sh port_number memory_size gc_opt enable_debug
#

port=$1
mem_size=$2
logfile="/home/ec2-user/log/$port"
echo "logging to $logfile"

gc_opt=$3
case $gc_opt in
  default)
    gc_flag=""
    ;;
  cms)
    gc_flag="-XX:-UseConcMarkSweepGC"
    ;;
  g1)
    gc_flag="-XX:+UseG1GC"
    ;;
  parallel)
    gc_flag="-XX:-UseParallelGC"
    ;;
  *)
    echo "unknown gc option $gc_opt"
    exit 1
    ;;
esac

JAVA_DEBUG_OPTS=""
if [[ $4 = "true" ]]; then
  source /home/ec2-user/replaydb-worker/setup_java_debug_opts.sh
fi

nohup java $JAVA_DEBUG_OPTS $gc_flag -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xmx$mem_size -Xms$mem_size -Dreplaydb-logdir=/home/ec2-user/log -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.WorkerService $port > $logfile.launch-out 2> $logfile.launch-err < /dev/null &
