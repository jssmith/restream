#!/bin/bash
#
# Launch an SSH tunnel out to an EC2 instance at an IP address specified as the argument to this script. Port 7091.
# Usage: ./launch_ssh_tunnel.sh remote.ip.address
#

if [[ $# -ne 1 ]]; then
  echo "Usage: ./launch_ssh_tunnel.sh remote.ip.address"
  exit
fi

ssh ec2-user@$1 -4NL 7091:localhost:7091 &
echo "Process ID of ssh, if you want to kill it later:"
ps aux | grep "7091:localhost:7091" | grep "ssh"

