#!/bin/bash
source /etc/profile

exec /letv/programs/spark-1.6.0-bin-hadoop2.6_security/bin/spark-submit \
    --master yarn-clienta \
    --num-executors 30 \
    --executor-cores 2 \
    --executor-memory 2G \
    --conf spark.app.name=StreamingETL \
    --conf spark.ui.port=13000 \
    --conf spark.streaming.blockInterval=1000ms \
    --conf spark.streaming.kafka.maxRatePerPartition=30000 \
    --conf spark.streaming.kafka.maxRetries=2 \
    --conf spark.yarn.jar=hdfs://alluxio-cluster/sparkLib/spark-assembly-1.6.0-hadoop2.6.0.jar \
    --conf spark.local.dir=/data/slot6/streamingetl/tmp \
    --conf spark.driver.extraJavaOptions=-Dconfig.file=/data/slot6/streamingetl/application.conf \
    --class org.interestinglab.waterdrop.WaterdropMain \
    /data/slot6/streamingetl/WaterdropMain-assembly-0.1.0.jar

