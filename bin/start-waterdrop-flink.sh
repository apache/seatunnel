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
PARAMS=""
while (( "$#" )); do
  case "$1" in
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

    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;

  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UTILS_DIR=${BIN_DIR}/utils
APP_DIR=$(dirname ${BIN_DIR})
CONF_DIR=${APP_DIR}/config
PLUGINS_DIR=${APP_DIR}/lib
DEFAULT_CONFIG=${CONF_DIR}/application.conf
CONFIG_FILE=${CONFIG_FILE:-$DEFAULT_CONFIG}


assemblyJarName=$(find ${PLUGINS_DIR} -name waterdrop-core*.jar)

source ${CONF_DIR}/waterdrop-env.sh

string_trim() {
    echo $1 | awk '{$1=$1;print}'
}

export JVM_ARGS=$(string_trim "${variables_substitution}")

exec ${FLINK_HOME}/bin/flink run \
    ${PARAMS} \
    -c io.github.interestinglab.waterdrop.WaterdropFlink \
    ${assemblyJarName} --config ${CONFIG_FILE}