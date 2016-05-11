
#!/bin/bash
#
# Launch a spark job 
# Usage: ./launch.sh master_url launch_class executor_memory size_spec num_splits 
#  e.g.: ./launch.sh 172.31.31.42 replaydb.SimpleSpamDetectorSpark 5g 5m 16
#

if [[ $# -lt 5 ]]; then
    echo "Launch a spark job "
    echo "Usage: ./launch.sh master_url launch_class executor_memory size_spec num_splits [ batches='' ]"
    echo " e.g.: ./launch.sh 172.31.31.42 replaydb.SimpleSpamDetectorSpark 5g 5m 16"
    exit
fi

master_url=$1
launch_class=$2
executor_mem=$3
size_spec=$4
num_splits=$5
if [[ $# -gt 5 ]]; then
  batches=$6
  batches_str=-$6
  extra_driver_stack= #"--conf \"spark.driver.extraJavaOptions=-Xss10m\" --conf \"spark.executor.extraJavaOptions=-Xss10m\""
else
  batches=
  batches_str=
  extra_driver_stack=
fi

JAVA_HOME=/opt/java /home/ec2-user/spark/bin/spark-submit --class $launch_class --master spark://$master_url:6066 --deploy-mode client $extra_driver_stack --conf "spark.executor.memory=$executor_mem" --conf "spark.local.dir=/home/ec2-user/data1" file:///home/ec2-user/replaydb-worker/replaydb-spark-apps-assembly-0.1-SNAPSHOT.jar $master_url /home/ec2-user/data0/events-$size_spec-split-$num_splits$batches_str/events.out $num_splits $batches
