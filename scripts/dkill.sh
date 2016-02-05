#!/bin/bash
#
# Kill all running Java programs
#

for host in `cat ~/conf/workers.txt`; do 
  echo "Killall on $host"
  ssh $host "bash -l -c 'killall -9 java; rm -f /home/ec2-user/log/*'"
done
