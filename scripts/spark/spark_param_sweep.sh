#!/bin/bash
#
# Script to easily do a parameter sweep of distributed driving of Spark apps

if [[ $# -lt 10 ]]; then
  echo "Usage: ./spark_param_sweep.sh master_url iterations size_spec num_batches batch_size_events batch_size_ms memsize host_counts detectors event_count"
  echo "       where all args except iterations and size_spec should be a space-separated"
  echo "       list of values to sweep over"
  echo ""
  echo "       num_batches will be used for non-streaming; batch_size_{ms,events} will be used for streaming"
  echo "       batch_size_events is per-partition"
  echo ""
  echo "       NOTE: detectors will automatically prepend \"replaydb.\""
  echo " e.g.: ./spark_param_sweep.sh 172.31.31.41 5 5m-each 1 3000 100000 3000m \"1 2 4 8\" \"SimpleSpamDetectorSparkExactBatches IpSpamDetectorSparkExactBatches\" 5000000"
  exit
fi

USE_DEBUG=true

master_url=$1
iterations=$2
size_spec=$3
num_batches=$4
batch_size_events=$5
batch_size_ms=$6
mem_size=$7
host_counts=$8
detectors=$9
events_per_partition=${10}

for iteration in `seq 1 $iterations`; do
  for partitions in $host_counts; do
    event_count=$((events_per_partition*partitions))
    cp $HOME/conf/workers-$partitions.txt /home/ec2-user/spark/conf/slaves
    for detector in $detectors; do
      if [[ $detector == *Streaming ]]; then
        batch_spec="$batch_size_events $batch_size_ms"
      else
        batch_spec=$num_batches
      fi
      $HOME/spark/sbin/start-all.sh
      echo "driving spark on $partitions partitions"
      echo "$HOME/replaydb/scripts/sparklaunch.sh $master_url replaydb.$detector $mem_size $size_spec $partitions $batch_spec $event_count >> sparkout.log"
      sleep 5;
      $HOME/replaydb/scripts/spark/sparklaunch.sh $master_url replaydb.$detector $mem_size $size_spec $partitions $batch_spec $event_count >> sparkout.log
      sleep 2;
      $HOME/spark/sbin/stop-all.sh
      aws s3 rm --recursive s3://spark-restream/checkpoint 1>/dev/null
      sleep 5;
      $HOME/replaydb/scripts/distcmd.sh "rm -rf /media/ephemeral1/spark"
      $HOME/replaydb/scripts/distcmd.sh "rm -rf /home/ec2-user/spark/logs/"
      rm -rf $HOME/spark/logs
      rm -rf /media/ephemeral1/spark
    done
  done
done
