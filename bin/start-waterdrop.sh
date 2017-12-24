#!/bin/bash

# TODO: 启动时传入--mode ${DEPLOY_MODE} --config ${CONFIG_FILE}参数
# TODO: 解析bash 参数...
# TODO: 解析2次参数，关心的参数是--master, --config
# TODO: 有可能请求资源大小的配置也要直接提供给spark-submit !!!
# TODO: 删除其他不用的main class starter
# TODO: compress plugins/ dir before start-waterdrop.sh

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/lib
PLUGINS_DIR=${APP_DIR}/plugins

DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${DEFAULT_CONFIG}

DEFAULT_MASTER=local[2]
MASTER=${DEFAULT_MASTER}

DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEFAULT_DEPLOY_MODE}

# scan jar dependencies for all plugins
source ${UTILS_DIR}/file.sh
jarDependencies=$(listJarDependenciesOfPlugins ${PLUGINS_DIR})
JarDepOpts=""
if [ "$jarDependencies" != "" ]; then
    JarDepOpts="--jars $jarDependencies"
fi

FilesDepOpts=""
if [ "$DEPLOY_MODE" == "cluster" ]; then

    ## add config file
    FilesDepOpts="--files ${CONFIG_FILE}"
    CONFIG_FILE=$(basename ${CONFIG_FILE})

    ## add plugin files
    FilesDepOpts="${FilesDepOpts},${APP_DIR}/plugins.tar.gz"

    echo ""

elif [ "$DEPLOY_MODE" == "client" ]; then

    echo ""
fi

assemblyJarName=$(find ${LIB_DIR} -name Waterdrop-*.jar)

source ${CONF_DIR}/waterdrop-env.sh

exec ${SPARK_HOME}/bin/spark-submit --class org.interestinglab.waterdrop.Waterdrop \
    --master ${MASTER} \
    --deploy-mode ${DEPLOY_MODE} \
    ${JarDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} --mode ${DEPLOY_MODE} --config ${CONFIG_FILE}