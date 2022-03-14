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
  echo "Usage: start-seatunnel-flink.sh [options]"
  echo "  options:"
  echo "    --config, -c FILE_PATH        Config file"
  echo "    --variable, -i PROP=VALUE     Variable substitution, such as -i city=beijing, or -i date=20190318"
  echo "    --check, -t                   Check config"
  echo "    --help, -h                    Show this help message"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]] || [[ $# -le 1 ]]; then
  usage
  exit 0
fi

is_exist() {
    if [ -z $1 ]; then
      usage
      exit -1
    fi
}

PARAMS=""
while (( "$#" )); do
  case "$1" in
    -c|--config)
      CONFIG_FILE=$2
      is_exist ${CONFIG_FILE}
      shift 2
      ;;

    -i|--variable)
      variable=$2
      is_exist ${variable}
      java_property_value="-D${variable}"
      variables_substitution="${java_property_value} ${variables_substitution}"
      shift 2
      ;;

    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;

  esac
done

if [ -z ${CONFIG_FILE} ]; then
  echo "Error: The following option is required: [-c | --config]"
  usage
  exit -1
elif [ ! -f ${CONFIG_FILE} ];then
  echo "Error: Config file ${CONFIG_FILE} does not exists! Please check it."
  exit -1
fi

# set positional arguments in their proper place
eval set -- "$PARAMS"

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
PLUGINS_DIR=${APP_DIR}/lib
DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}

assemblyJarName=$(find ${PLUGINS_DIR} -name seatunnel-core-flink*.jar)

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    source ${CONF_DIR}/seatunnel-env.sh
fi

string_trim() {
    echo $1 | awk '{$1=$1;print}'
}

export JVM_ARGS=$(string_trim "${variables_substitution}")

exec ${FLINK_HOME}/bin/flink run \
    ${PARAMS} \
    -c org.apache.seatunnel.SeatunnelFlink \
    ${assemblyJarName} --config ${CONFIG_FILE}
