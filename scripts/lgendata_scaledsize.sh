#!/bin/bash
#
# Locally generate data 
# Note that in this case, num_events should be the number of events *per split*
# Usage: ./lgendata_scaledsize.sh size_spec num_events num_users split_nums [ keep_only=-1 ]
#  e.g.: ./lgendata_scaledsize.sh 5m-each 5000000 100000 "1 2 4 8 16 32" 1
#        
#        keep_only denotes the denotes the *only* output partition that should 
#        actually be saved to disk (-1 for all). should be this host's partition ID
#

ROOT_DIR=/home/ec2-user/data0

if [[ $# -lt 4 ]]; then
  echo "Usage: ./lgendata_scaledsize.sh size_spec num_events num_users split_nums [ keep_only=-1 ]"
  echo "e.g.: ./lgendata_scaledsize.sh 5m-each 5000000 100000 \"1 2 4 8 16 32\" 1"
  echo "   keep_only denotes the denotes the *only* output partition that should "
  echo "   actually be saved to disk (-1 for all). should be this host's partition ID"
  echo ""
  echo "Note that, in this case, num_events should be the number of events *per split*"
  exit
fi

SIZE_SPEC=$1
EVENTS_PER_SPLIT=$2
NUM_USERS=$3
SPLIT_NUMS=$4
if [ $# -ge 5 ]; then
  KEEP_ONLY=$5
else
  KEEP_ONLY=-1
fi

for i in ${SPLIT_NUMS[*]}; do
  NUM_EVENTS=$(($EVENTS_PER_SPLIT*$i))
  rm -rf $ROOT_DIR/events-$SIZE_SPEC-split-$i
  java -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.EventGenerator tunable $NUM_USERS $NUM_EVENTS $ROOT_DIR/events-$SIZE_SPEC-split-$i/events.out $i $KEEP_ONLY
done

