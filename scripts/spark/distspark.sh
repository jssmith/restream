#!/bin/bash
#
# Distribute spark code to workers
#

for host in `cat $HOME/conf/workers.txt`; do 
  scp /home/ec2-user/spark-1.6.1-bin-restream-spark.tgz \
    $host:
  ssh $host "bash -l -c 'tar -xzf spark-1.6.1-bin-restream-spark.tgz; mv spark-1.6.1-bin-restream-spark spark; '"
done

