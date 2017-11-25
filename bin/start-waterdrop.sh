#!/bin/bash

SPARK_HOME=${SPARK_HOME:-/opt/spark}


BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=$BIN_DIR/utils
APP_DIR=$(dirname $BIN_DIR)
CONF_DIR=$APP_DIR/config
LIB_DIR=$APP_DIR/lib

source $UTILS_DIR/file.sh

assemblyJarName=$(find $LIB_DIR -name Waterdrop-*.jar)

exec $SPARK_HOME/bin/spark-submit --class org.interestinglab.waterdrop.Waterdrop \
    --conf spark.driver.extraJavaOptions=-Dconfig.path=$CONF_DIR/application.conf \
    $assemblyJarName

