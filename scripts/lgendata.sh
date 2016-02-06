#!/bin/bash
#
# Locally generate data 
# Usage: ./lgendata.sh size_spec num_events num_users split_nums
#  e.g.: ./lgendata.sh 50m 50000000 100000 "1 2 4 8 16 32"
#

SIZE_SPEC=$1
NUM_EVENTS=$2
NUM_USERS=$3
SPLIT_NUMS=$4


if [[ $# -ne 4 ]]; then
  echo "Usage: ./lgendata.sh size_spec num_events num_users split_nums"
  echo "e.g.: ./lgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\""
  exit
fi

for i in ${SPLIT_NUMS[*]}; do
  rm -rf /home/ec2-user/data/events-$SIZE_SPEC-split-$i
  java -cp /home/ec2-user/replaydb-worker/replaydb-apps-assembly-0.1-SNAPSHOT.jar replaydb.exec.EventGenerator tunable $NUM_USERS $NUM_EVENTS /home/ec2-user/data/events-$SIZE_SPEC-split-$i/events.out $i
done
