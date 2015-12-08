#!/bin/bash

SPAM_DETECTOR=$1
SIZE_SPEC=$2
NUM=$3

echo "Driving $SPAM_DETECTOR with $NUM partitions"

time java -Xmx2000m \
  -cp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  replaydb.exec.spam.DistributedSpamDetector $SPAM_DETECTOR \
  /home/ec2-user/data/events-$SIZE_SPEC-split-$NUM/events.out \
  $NUM 10000 /home/ec2-user/conf/latesthosts.txt

