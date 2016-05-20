#!/bin/bash
#
# Generate data on all workers, and locally (truncated to the length size_spec,
#     i.e. length used with one partition)
# In this case, num_events should be the number of events *per split*
# Usage: ./dgendata_scaledsize.sh size_spec num_events num_users split_nums [ keep_only=false ]
#  e.g.: ./dgendata_scaledsize.sh 5m-each 5000000 100000 "1 2 4 8 16 32" true
#
#  if keep_only is true, each host will keep only the data relevant to its partition ID
#  note that this will only work if each host is running only one partition

if [[ $# -lt 4 ]]; then
  echo "Usage: ./dgendata_scaledsize.sh size_spec num_events num_users split_nums [ batches=1 ] [ keep_only=false ] [ partitioned=false ] [ alpha=1.0 ]"
  echo "e.g.: ./dgendata_scaledsize.sh 5m-each 5000000 100000 \"1 2 4 8 16 32\" 1 true true"
  echo ""
  echo "batches denotes the number of batches to break the files into (ONLY relevant for spark)"
  echo ""
  echo "Note that in this case, num_events should be t:q!he number of events *per split*"
  echo ""
  echo "if keep_only is true, each host will keep only the data relevant to its partition ID"
  echo "note that this will only work if each host is running only one partition"
  exit
fi

if [ $# -ge 5 ]; then
  batches=$5
else
  batches=1
fi
if [ $# -ge 6 ]; then
  keep_only=$6
else
  keep_only=false
fi
if [ $# -ge 7 ]; then
  partitioned=$7
else
  partitioned=false
fi
if [ $# -ge 8 ]; then
  ALPHA=$8
else
  ALPHA=1.0
fi
df
echo "generating data remotely, $2 events per partition"
idx=0
for host in `cat $HOME/conf/workers.txt`; do
  if [ $keep_only = true ]; then
    keep_only_arg=$idx
  else
    keep_only_arg=-1
  fi
  ssh $host "bash -l -c '/home/ec2-user/replaydb-worker/lgendata_scaledsize.sh $1 $2 $3 \"$4\" $batches $keep_only_arg $partitioned $ALPHA'" &
  idx=$(($idx+1))
done

truncated_size=$2
echo "generating data locally, $truncated_size events"
$HOME/replaydb-worker/lgendata.sh $1 $truncated_size $3 "$4" 1 -1 false $ALPHA
