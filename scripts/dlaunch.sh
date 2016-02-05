#!/bin/bash
#
# Distributed launch. Starts the workers.
#

NUM=$1
echo "Launching $NUM services"

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
  ssh $HOST "bash /home/ec2-user/replaydb-worker/llaunch.sh $port"
  echo "$HOST:$port" >> /home/ec2-user/conf/latesthosts.txt
done
