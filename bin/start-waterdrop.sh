#!/bin/bash

# TODO: distinguish master/deploy-mode
# TODO: allowed master values: local, yarn-client, yarn-master, mesos:???
# TODO: handle --config $CONFIG_FILE relative/absoulte path according to master/deploy-mode

# SPARK_HOME=${SPARK_HOME:-/opt/spark}
BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=$BIN_DIR/utils
APP_DIR=$(dirname $BIN_DIR)
CONF_DIR=$APP_DIR/config
LIB_DIR=$APP_DIR/lib
PLUGINS_DIR=$APP_DIR/plugins

# scan jar dependencies for all plugins
source $UTILS_DIR/file.sh
jarDependencies=$(listJarDependenciesOfPlugins $PLUGINS_DIR)
JarDepOpts=""
if [ "$jarDependencies" != "" ]; then
    JarDepOpts="--jars $jarDependencies"
fi

DEFAULT_MASTER=local[2]
DEFAULT_CONFIG=$CONF_DIR/application.conf

MASTER=$DEFAULT_MASTER
CONFIG_FILE=$DEFAULT_CONFIG

assemblyJarName=$(find $LIB_DIR -name Waterdrop-*.jar)

source $CONF_DIR/waterdrop-env.sh

exec $SPARK_HOME/bin/spark-submit --class org.interestinglab.waterdrop.Waterdrop \
    --master $MASTER \
    ${JarDepOpts} \
    $assemblyJarName --master $MASTER --config $CONFIG_FILE