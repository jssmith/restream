#!/bin/bash

NUM=$1

echo "Driving with $NUM partitions"

time java -Xmx2000m \
  -cp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  replaydb.exec.spam.DistributedSpamDetector /home/ec2-user/data/events-short-split-$NUM/events.out \
  $NUM 50000 /home/ec2-user/conf/latesthosts.txt

