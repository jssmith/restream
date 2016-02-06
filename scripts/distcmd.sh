#!/bin/bash
#
# Run some bash command (the argument to this script) on all workers
#

for host in `cat $HOME/conf/workers.txt`; do
  ssh $host "bash -l -c '$1'"
done

