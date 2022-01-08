#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# copy command line arguments

function usage() {
  echo "Usage: start-seatunnel-spark.sh [options]"
  echo "  options:"
  echo "    --deploy-mode,-e DEPLOY_MODE  Spark deploy mode"
  echo "    --master, -m MASTER_URL       Spark master"
  echo "    --config, -c FILE_PATH        Config file"
  echo "    --variable, -i PROP=VALUE     Variable substitution, such as -i city=beijing, or -i date=20190318"
  echo "    --check, -t                   Check config"
  echo "    --help, -h                    Show this help message"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]] || [[ $# -le 1 ]]; then
  usage
  exit 0
fi

CMD_ARGUMENTS=$@

PARAMS=""
while (( "$#" )); do
  case "$1" in
    -m|--master)
      MASTER=$2
      shift 2
      ;;

    -e|--deploy-mode)
      DEPLOY_MODE=$2
      shift 2
      ;;

    -c|--config)
      CONFIG_FILE=$2
      shift 2
      ;;

    -i|--variable)
      variable=$2
      java_property_value="-D${variable}"
      variables_substitution="${java_property_value} ${variables_substitution}"
      shift 2
      ;;

    --) # end argument parsing
      shift
      break
      ;;

    # -*|--*=) # unsupported flags
    #  echo "Error: Unsupported flag $1" >&2
    #  exit 1
    #  ;;

    *) # preserve positional arguments
      PARAM="$PARAMS $1"
      shift
      ;;

  esac
done

if [ -z ${MASTER} ] || [ -z ${DEPLOY_MODE} ] || [ -z ${CONFIG_FILE} ]; then
  echo "Error: The following options are required: [-e | --deploy-mode], [-m | --master], [-c | --config]"
  usage
  exit -1
fi

# set positional arguments in their proper place
eval set -- "$PARAMS"


BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
LIB_DIR=${APP_DIR}/lib
PLUGINS_DIR=${APP_DIR}/plugins

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

assemblyJarName=$(find ${LIB_DIR} -name seatunnel-core-spark*.jar)

string_trim() {
    echo $1 | awk '{$1=$1;print}'
}

variables_substitution=$(string_trim "${variables_substitution}")

## get spark conf from config file and specify them in spark-submit
function get_spark_conf {
    spark_conf=$(java ${variables_substitution} -cp ${assemblyJarName} org.apache.seatunnel.config.ExposeSparkConf ${CONFIG_FILE})
    if [ "$?" != "0" ]; then
        echo "[ERROR] config file does not exists or cannot be parsed due to invalid format"
        exit -1
    fi

    echo ${spark_conf}
}

sparkconf=$(get_spark_conf)

echo "[INFO] spark conf: ${sparkconf}"

# Spark Driver Options
driverJavaOpts=""
executorJavaOpts=""
clientModeDriverJavaOpts=""
if [ ! -z "${variables_substitution}" ]; then
  driverJavaOpts="${variables_substitution}"
  executorJavaOpts="${variables_substitution}"
  # in local, client mode, driverJavaOpts can not work, we must use --driver-java-options
  clientModeDriverJavaOpts="${variables_substitution}"
fi


## compress plugins.tar.gz in cluster mode
if [ "${DEPLOY_MODE}" == "cluster" ]; then

  plugins_tar_gz="${APP_DIR}/plugins.tar.gz"

  if [ ! -f "${plugins_tar_gz}" ]; then
    cur_dir=$(pwd)
    cd ${APP_DIR}
    tar zcf plugins.tar.gz plugins
    if [ "$?" != "0" ]; then
      echo "[ERROR] failed to compress plugins.tar.gz in cluster mode"
      exit -2
    fi

    echo "[INFO] successfully compressed plugins.tar.gz in cluster mode"
    cd ${cur_dir}
  fi
fi

exec ${SPARK_HOME}/bin/spark-submit --class org.apache.seatunnel.SeatunnelSpark \
    --name $(getAppName ${CONFIG_FILE}) \
    --jars $(echo ${LIB_DIR}/*.jar | tr ' ' ',') \
    --master ${MASTER} \
    --deploy-mode ${DEPLOY_MODE} \
    --driver-java-options "${clientModeDriverJavaOpts}" \
    --conf spark.executor.extraJavaOptions="${executorJavaOpts}" \
    --conf spark.driver.extraJavaOptions="${driverJavaOpts}" \
    ${sparkconf} \
    ${JarDepOpts} \
    ${FilesDepOpts} \
    ${assemblyJarName} ${CMD_ARGUMENTS}
