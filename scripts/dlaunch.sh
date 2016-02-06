#!/bin/bash
#
# Distributed launch. Starts the workers.
# Usage: ./dlaunch.sh num_workers memory_size enable_debug
#  e.g.: ./dlaunch.sh 12 6000m true
#

NUM=$1
echo "Launching $NUM services"

$mem_size=$2
$use_debug=$3

if [[ $# -ne 3 ]]; then
  echo "Usage: ./dlaunch.sh num_workers memory_size enable_debug"
  echo " e.g.: ./dlaunch.sh 12 6000m true"
  exit
fi

PORT_START=5567
PORT_END=`expr $PORT_START + $NUM - 1`

readarray WORKERS < /home/ec2-user/conf/workers.txt

NUM_WORKERS=${#WORKERS[@]}

rm /home/ec2-user/conf/latesthosts.txt

for port in `seq $PORT_START $PORT_END`; do
  HOSTINDEX=$((port%NUM_WORKERS))
  HOST=${WORKERS[$HOSTINDEX]}
  HOST=`echo $HOST`
  echo "Launch on $HOST:$port"
  ssh $HOST "bash /home/ec2-user/replaydb-worker/llaunch.sh $port $mem_size $use_debug"
  echo "$HOST:$port" >> /home/ec2-user/conf/latesthosts.txt
done
