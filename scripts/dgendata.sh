#!/bin/bash
#
# Generate data on all workers, and locally (truncated to 1/50th the length) 
# Usage: ./dgendata.sh size_spec num_events num_users split_nums
#  e.g.: ./dgendata.sh 50m 50000000 100000 "1 2 4 8 16 32"

if [[ $# -ne 4 ]]; then
  echo "Usage: ./dgendata.sh size_spec num_events num_users split_nums"
  echo "e.g.: ./dgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\""
  exit
fi

echo "generating data locally, $2 events"
idx=0
for host in `cat $HOME/conf/workers.txt`; do
  ssh $host "bash -l -c '/home/ec2-user/replaydb-worker/lgendata.sh $1 $2 $3 \"$4\" $idx'" &
  idx=$(($idx+1))
done

truncated_size=$(($2/50))
echo "generating data locally, $truncated_size events"
$HOME/replaydb-worker/lgendata.sh $1 $truncated_size $3 "$4" $5
