#!/bin/bash
#
# Launch the parallel spam detector (single host multi-threaded version)
#

SPAM_DETECTOR=$1
SIZE_SPEC=$2
N=$3

time \
  java -Xmx24G -XX:+UseConcMarkSweepGC \
  -cp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  replaydb.exec.spam.ParallelSpamDetector $SPAM_DETECTOR \
  /home/ec2-user/data/events-$SIZE_SPEC-split-$N/events.out $N 500 50000
