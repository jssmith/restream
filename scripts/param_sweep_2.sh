#!/bin/bash
#
# Script to easily do a parameter sweep of distributed driving
# Usage: ./param_sweep.sh iterations size_spec host_counts detectors [ batch_sizes=10000 ]
#        where all args except iterations and size_spec should be a space-separated
#        list of values to sweep over
#        NOTE: detectors will automatically prepend "replaydb.exec.spam."
#  e.g.: ./param_sweep.sh 10 5m 3000m "1 2 4 8" "SimpleSpamDetectorStats IpSpamDetectorStats"
#

if [[ $# -lt 5 ]]; then
  echo "Usage: ./param_sweep_2.sh iterations size_spec memsize host_counts detectors [ batch_sizes=10000 ] [ alphas=2.0 ] [ logarchive = s3://edu.berkeley.restream/log ]"
  echo "       where all args except iterations and size_spec should be a space-separated"
  echo "       list of values to sweep over"
  echo "       NOTE: detectors will automatically prepend \"replaydb.exec.spam.\""
  echo " e.g.: ./param_sweep.sh 10 5m 3000m \"1 2 4 8\" \"SimpleSpamDetectorStats IpSpamDetectorStats\" \"10000 50000\" \"1.75 2.0 2.25\" s3://edu.berkeley.restream/log"
  exit
fi

USE_DEBUG=true

iterations=$1
size_spec=$2
mem_size=$3
host_counts=$4
detectors=$5
if [ $# -ge 6 ]; then
  batch_sizes=$6
else
  batch_sizes=10000
fi
if [ $# -ge 7 ]; then
  alphas=$7
else
  alphas='2.0'
fi
if [ $# -ge 8 ]; then
  log_archive=$8
else
  log_archive='s3://edu.berkeley.restream/log'
fi

for iteration in `seq 1 $iterations`; do
  for nhosts in $host_counts; do
    cp $HOME/conf/workers-$nhosts.txt /home/ec2-user/conf/workers.txt
    partitions=$nhosts
    for detector in $detectors; do
      for batch_size in $batch_sizes; do
      partitioned=false
      #for partitioned in false true; do
      for alpha in $alphas; do
        UUID=$(uuidgen)
        $HOME/replaydb/scripts/dkill.sh
        $HOME/replaydb/scripts/dlaunch.sh $partitions $mem_size default $USE_DEBUG
        echo "lanched $partitions partitions on $nhosts hosts"
        sleep 2;
        fnbase="$UUID-$nhosts-$partitions-$iteration-$detector-$partitioned-$batch_size-$alpha"
        $HOME/replaydb/scripts/ddrive.sh $detector $size_spec-$alpha $partitions $batch_size true $partitioned > $fnbase.txt 2>> $fnbase.timing
        pssh -i -h ~/conf/workers.txt 'gzip log/*.log'
        pssh -i -h ~/conf/workers.txt "aws s3 sync log $log_archive/$UUID"
      done
      done
    done
  done
done
