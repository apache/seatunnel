#!/bin/bash

### # functions
usage () {
  local l_MSG=$1
  echo "Usage Error: $l_MSG"
  echo "Usage: $0 Logfile-stem"
  exit 1
}

# args: (1) -master master, (2) -f config file (3) -t config test(non-value)
# TODO: command arguments parser
# TODO: how to specify SPARK_HOME ?
# TODO: allowed master values: local, yarn-client, yarn-master, mesos:???

SPARK_HOME=${SPARK_HOME:-/opt/spark}
BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=$BIN_DIR/utils
APP_DIR=$(dirname $BIN_DIR)
CONF_DIR=$APP_DIR/config
LIB_DIR=$APP_DIR/lib

source $UTILS_DIR/file.sh

DEFAULT_MASTER=local[2]
DEFAULT_CONFIG=$CONF_DIR/application.conf

MASTER=$DEFAULT_MASTER
CONFIG_FILE=$DEFAULT_CONFIG

assemblyJarName=$(find $LIB_DIR -name Waterdrop-*.jar)

exec $SPARK_HOME/bin/spark-submit --class org.interestinglab.waterdrop.Waterdrop \
    --master $MASTER \
    $assemblyJarName --master $MASTER --file $CONFIG_FILE

