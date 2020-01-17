#!/bin/bash
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