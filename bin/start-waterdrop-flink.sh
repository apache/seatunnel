#!/bin/bash
# copy command line arguments
CMD_ARGUMENTS=$@
BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/libs
PLUGINS_DIR=${APP_DIR}/modules
DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}
DEFAULT_MASTER=local[2]
MASTER=${MASTER:-$DEFAULT_MASTER}
DEFAULT_DEPLOY_MODE=client
DEPLOY_MODE=${DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}
# scan jar dependencies for all plugins
source ${UTILS_DIR}/file.sh
source ${UTILS_DIR}/app.sh
jarDependencies=$(listJarDependenciesOfPlugins ${PLUGINS_DIR})
JarDepOpts=""
if [ "$jarDependencies" != "" ]; then
    JarDepOpts="--jars $jarDependencies"
fi
FilesDepOpts=""
if [ "$DEPLOY_MODE" == "cluster" ]; then
    ## add config file
    FilesDepOpts="--files ${CONFIG_FILE}"
    ## add plugin files
    FilesDepOpts="${FilesDepOpts},${APP_DIR}/plugins.tar.gz"
    echo ""
elif [ "$DEPLOY_MODE" == "client" ]; then
    echo ""
fi
assemblyJarName=$(find ${PLUGINS_DIR} -name waterdrop-core*.jar)

source ${CONF_DIR}/waterdrop-env.sh
string_trim() {
    echo $1 | awk '{$1=$1;print}'
}
variables_substitution=$(string_trim "${variables_substitution}")

echo ${assemblyJarName}
exec ${FLINK_HOME}/bin/flink run\
    $(echo ${LIB_DIR}/*.jar |awk -F ' ' '{for(i=1;i<NF;i++) print "-C file://" $i}'|xargs) \
    $(echo ${PLUGINS_DIR}/*.jar |awk -F ' ' '{for(i=1;i<NF;i++) print "-C file://" $i}'|xargs) \
    -c io.github.interestinglab.waterdrop.WaterdropFlink \
    ${assemblyJarName} ${CMD_ARGUMENTS}
