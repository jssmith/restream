#!/bin/bash

for host in `cat ~/conf/workers.txt`; do 
  echo "Killall on $host"
  ssh $host "bash -l -c 'killall -9 java'"
done
