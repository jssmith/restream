#!/bin/bash
#
# Generate data on all workers 
# Usage: ./dgendata.sh size_spec num_events num_users split_nums
#  e.g.: ./dgendata.sh 50m 50000000 100000 "1 2 4 8 16 32"
#

if [[ $# -ne 4 ]]; then
  echo "Usage: ./dgendata.sh size_spec num_events num_users split_nums"
  echo "e.g.: ./dgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\""
  exit
fi

for host in `cat $HOME/conf/workers.txt`; do
  ssh $host "bash -l -c '/home/ec2-user/replaydb-worker/lgendata.sh $1 $2 $3 \"$4\"'" &
done
