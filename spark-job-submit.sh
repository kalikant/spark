#!/bin/bash

param1=$1
param2=$2
param3=$3

echo "args: param1->$param1 param2->$param2 param3->$param3"

export APP_NAME=spark-app
export BASE_DIR=/home/spark-app
export LOG_LEVEL=INFO
export CONF_DIR=${BASE_DIR}/conf
export LOG_PATH=${BASE_DIR}/logs

(java -cp "/home/spark-app/spark-jars/*:/home/spark-app/lib/*:/home/spark-app/spark-app.jar" org.util.Encryption $textToencrypt $seedPWD) > ${BASE_DIR}/tmp/pwd.enc
hdfs dfs -put -f ${BASE_DIR}/tmp/pwd.enc /hdfs/main/directory/etc/sec
rm ${BASE_DIR}/tmp/pwd.enc

hdfs dfs -chmod 764 /hdfs/main/directory/etc/sec/*

libs=$(echo ${BASE_DIR}/lib/*.jar | tr ' ' ',')
spark_log_file="spark_log_""$param3"`date -d today '+%Y%m%d%H%M%S'`

/bin/spark2-submit \
    --class org.AppMain \
    --jars ${libs} \
    --deploy-mode cluster \
    --master yarn \
    --queue root.your.queue.name \
    --driver-memory 3G 
	--executor-memory 10G \
    --num-executors 20 \
    --supervise \
    --conf spark.unsafe.sorter.spill.read.ahead.enabled=false \
    --files "${CONF_DIR}/log4j.properties" \
    --files /etc/hive/conf/hive-site.xml \
    --name ${APP_NAME} \
    ${BASE_DIR}/spark-app.jar $param1 $param2 $param3 2>&1 | tee -a $LOG_PATH/$spark_log_file &

status="INITIATED"
job_running="true"
exit_status=0
while [ $job_running == "true" ]
do
 sleep 5
 app_id=`cat $LOG_PATH/$spark_log_file | grep 'yarn.Client: Submitting application' | awk -F ' ' '{print $7}'`
 status=`yarn application -status $app_id | grep 'Final-State :' | awk -F ':' '{print $2}' | sed 's/ //g'`
 echo "status of application $app_id is $status"
 if [ "$status" == "FAILED" ]; then
  echo "Spark Job Failed.. please check log"
  job_running="false"
  exit_status=1
 elif [ "$status" == "SUCCEEDED" ]; then
  echo "Spark Job Completed Successfully .."
  job_running="false"
  exit_status=0
 fi
done

echo "cleaning hdfs directory.."
hdfs dfs -rm -skipTrash /hdfs/main/directory/etc/sec/*
exit $exit_status
