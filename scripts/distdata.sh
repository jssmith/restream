#!/bin/bash

# Distribute data to workers

for host in `cat $HOME/conf/workers.txt`; do
  ssh $host "bash -l -c 'rm -rf $HOME/data'"
  scp -r $HOME/src-data $host:data
done

