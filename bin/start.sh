#!/bin/bash
source /etc/profile

exec $SPARK_HOME/bin/spark-submit \
    --master yarn-clienta \
    --num-executors 30 \
    --executor-cores 2 \
    --executor-memory 2G \
    --conf spark.app.name=Waterdrop \
    --conf spark.ui.port=13000 \
    --conf spark.streaming.blockInterval=1000ms \
    --conf spark.streaming.kafka.maxRatePerPartition=30000 \
    --conf spark.streaming.kafka.maxRetries=2 \
    --conf spark.driver.extraJavaOptions=-Dconfig.file=/data/slot6/waterdrop/application.conf \
    --class org.interestinglab.waterdrop.WaterdropMain \
    /data/slot6/waterdrop/WaterdropMain-assembly-0.1.0.jar

