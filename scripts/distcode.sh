#!/bin/bash
#
# Distribute code to workers
#

for host in `cat $HOME/conf/workers.txt`; do 
  scp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  $HOME/replaydb/scripts/llaunch.sh \
  $HOME/replaydb/scripts/lgendata.sh \
  $HOME/replaydb/scripts/setup_java_debug_opts.sh \
    $host:replaydb-worker
done

# Also distribute to self so that commands expecting that code work on master as well 
cp /home/ec2-user/replaydb/apps/target/scala-2.11/replaydb-apps-assembly-0.1-SNAPSHOT.jar \
  $HOME/replaydb/scripts/llaunch.sh \
  $HOME/replaydb/scripts/lgendata.sh \
  $HOME/replaydb/scripts/setup_java_debug_opts.sh \
    $HOME/replaydb-worker
