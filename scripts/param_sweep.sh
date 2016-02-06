#!/bin/bash
#
# Script to easily do a parameter sweep of distributed driving 
# Usage: ./param_sweep.sh iterations host_counts jvms_per_host detectors
#        where all args except iterations should be a space-separated
#        list of values to sweep over
#  e.g.: ./param_sweep.sh 10 "1 2 4 8" "1 2 3 4" "replaydb.exec.spam.SimpleSpamDetectorStats replaydb.exec.spam.SimpleSpamDetectorStats"
#

if [[ $# -ne 4 ]]; then
  echo "Usage: ./lgendata.sh size_spec num_events num_users split_nums"
  echo "e.g.: ./lgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\""
  exit
fi

$MEM_SIZE=6000m
$USE_DEBUG=true

$iterations=$1
$host_counts=$2
$jvms_per_host=$3
$detectors=$4

for iteration in `seq 1 $iterations`; do
  for nhosts in $host_counts; do
    cp /home/ec2-user/conf/workers-$nhosts.txt /home/ec2-user/conf/workers.txt
    for jvmsperhost in $jvms_per_host; do
      partitions=$((nhosts*jvmsperhost))
      for detector in $detectors; do
        dkill.sh; dlaunch.sh $partitions $MEM_SIZE $USE_DEBUG
        echo "lanched $partitions partitions on $nhosts hosts"
        sleep 2;
        fnbase="$nhosts-$partitions-$iteration-$detector"
        ddrive.sh $detector 5m $partitions > $fnbase.txt 2>> $fnbase.timing
        mkdir $fnbase-log
        for host in `cat ~/conf/workers.txt`; do
          scp $host:log/* $fnbase-log
        done
      done
    done
  done
done

