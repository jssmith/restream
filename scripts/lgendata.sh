#!/bin/bash
#
# Locally generate data
# Usage: ./lgendata.sh size_spec num_events num_users split_nums [ keep_only=-1 ]
#  e.g.: ./lgendata.sh 50m 50000000 100000 "1 2 4 8 16 32" 1
#
#        keep_only denotes the denotes the *only* output partition that should
#        actually be saved to disk (-1 for all). should be this host's partition ID
#

ROOT_DIR=/home/ec2-user/data0

if [[ $# -lt 4 ]]; then
  echo "Usage: ./lgendata.sh size_spec num_events num_users split_nums [ keep_only=-1 ] [ partitioned=false ]"
  echo "e.g.: ./lgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\" 1 true"
  echo "   keep_only denotes the denotes the *only* output partition that should "
  echo "   actually be saved to disk (-1 for all). should be this host's partition ID"
  exit
fi

SIZE_SPEC=$1
NUM_EVENTS=$2
NUM_USERS=$3
SPLIT_NUMS=$4
if [ $# -ge 5 ]; then
  KEEP_ONLY=$5
else
  KEEP_ONLY=-1
fi
if [ $# -ge 6 ]; then
  PARTITIONED=$6
else
  PARTITIONED=false
fi
if [ $PARTITIONED = true ]; then
  DATA_PARTITION_SUFFIX=-part
else
  DATA_PARTITION_SUFFIX=
fi

for i in ${SPLIT_NUMS[*]}; do
  rm -rf $ROOT_DIR/events-$SIZE_SPEC-split-$i
  java -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.EventGenerator \
    tunable $NUM_USERS $NUM_EVENTS $ROOT_DIR/events-$SIZE_SPEC-split-$i$DATA_PARTITION_SUFFIX/events.out $i $KEEP_ONLY $PARTITIONED
done
