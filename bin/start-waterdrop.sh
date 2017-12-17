#!/bin/bash

# TODO: 启动时传入--mode ${DEPLOY_MODE} --config ${CONFIG_FILE}参数
# TODO: 解析bash 参数...
# TODO: cluster模式下addfile(config files, plugin/files), addJar(core jars, plugin/jars),
# TODO: 解析2次参数，关心的参数是--master, --config
# TODO: --config 参数需要在cluster模式下 改变path,
# TODO: 在cluster模式下，代码中的addFiles是否还有用!!!! 在cluster模式下是否还管用? 因为cluster模式下，driver已经运行在cluster上，无法再add local file

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

    ## add plugin files
    FilesDepOpts="--files ${CONFIG_FILE}"
    ## TODO: add directory, not only files !!!!!
    CONFIG_FILE=$(basename ${CONFIG_FILE})
    echo ""

elif [ "$DEPLOY_MODE" == "client" ]; then

    ## add plugin files
    ## TODO
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