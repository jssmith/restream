#!/bin/bash

for host in `cat $HOME/conf/workers.txt`; do 
  scp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  $HOME/replaydb/scripts/llaunch.sh \
    $host:replaydb-worker
done

