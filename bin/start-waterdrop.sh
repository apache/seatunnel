#!/bin/bash

exec $SPARK_HOME/bin/spark-submit \
    --class org.interestinglab.waterdrop.WaterdropMain \
    /data/slot6/waterdrop/WaterdropMain-assembly-0.1.0.jar

