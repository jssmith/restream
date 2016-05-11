#!/bin/bash
#
# Script to easily do a parameter sweep of distributed driving of Spark apps

if [[ $# -lt 5 ]]; then
  echo "Usage: ./spark_param_sweep.sh master_url iterations size_spec num_batches memsize host_counts detectors"
  echo "       where all args except iterations and size_spec should be a space-separated"
  echo "       list of values to sweep over"
  echo "       NOTE: detectors will automatically prepend \"replaydb.\""
  echo " e.g.: ./spark_param_sweep.sh 172.31.31.41 10 5m 1 3000m \"1 2 4 8\" \"SimpleSpamDetectorSparkExactBatches IpSpamDetectorSparkExactBatches\""
  exit
fi

USE_DEBUG=true

master_url=$1
iterations=$2
size_spec=$3
num_batches=$4
mem_size=$5
host_counts=$6
detectors=$7

for iteration in `seq 1 $iterations`; do
  for partitions in $host_counts; do
    cp $HOME/conf/workers-$nhosts.txt /home/ec2-user/spark/conf/slaves
    $HOME/spark/sbin/start-all.sh
    for detector in $detectors; do
      echo "driving spark on $partitions partitions"
      sleep 2;
      $HOME/replaydb/scripts/sparklaunch.sh $master_url replaydb.$detector $mem_size $size_spec $partitions $num_batches >> sparkout.log
      aws s3 rm --recursive s3://spark-restream/checkpoint 1>/dev/null
    done
    $HOME/spark/sbin/stop-all.sh
    sleep 2;
  done
done
