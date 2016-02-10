#!/bin/bash
#
# Generate data on all workers 
# Usage: ./dgendata.sh size_spec num_events num_users split_nums
#  e.g.: ./dgendata.sh 50m 50000000 100000 "1 2 4 8 16 32" -1
#
#        keep_only denotes the denotes the *only* output partition that should 
#        actually be saved to disk (-1 for all). should be this host's partition ID
#

if [[ $# -ne 4 ]]; then
  echo "Usage: ./dgendata.sh size_spec num_events num_users split_nums keep_only"
  echo "e.g.: ./dgendata.sh 50m 50000000 100000 \"1 2 4 8 16 32\" -1"
  echo "   keep_only denotes the denotes the *only* output partition that should "
  echo "   actually be saved to disk (-1 for all). should be this host's partition ID"
  exit
fi

for host in `cat $HOME/conf/workers.txt`; do
  ssh $host "bash -l -c '/home/ec2-user/replaydb-worker/lgendata.sh $1 $2 $3 \"$4\" $5'" &
done
