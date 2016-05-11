#!/bin/bash
#
# Distribute ssh key to workers
#

for host in `cat $HOME/conf/workers.txt`; do
  scp $HOME/.ssh/id_rsa.pub $host:master_rsa.pub
  ssh $host "bash -l -c 'cat master_rsa.pub >> /home/ec2-user/.ssh/authorized_keys'"
done

