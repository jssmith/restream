#!/bin/bash
#
# Distribute spark-apps code to workers
#

for host in `cat $HOME/conf/workers.txt`; do 
  scp /home/ec2-user/replaydb/spark-apps/target/scala-2.11/replaydb-spark-apps-assembly-0.1-SNAPSHOT.jar \
    $host:replaydb-worker
done

# Also distribute to self so that commands expecting that code work on master as well 
cp /home/ec2-user/replaydb/spark-apps/target/scala-2.11/replaydb-spark-apps-assembly-0.1-SNAPSHOT.jar \
    $HOME/replaydb-worker
