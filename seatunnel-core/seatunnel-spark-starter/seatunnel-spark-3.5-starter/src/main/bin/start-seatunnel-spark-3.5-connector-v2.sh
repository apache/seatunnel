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
set -eu

PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=`dirname "$PRG"`
APP_DIR=`cd "$PRG_DIR/.." >/dev/null; pwd`
CONF_DIR=${APP_DIR}/config
APP_JAR_NAME=seatunnel-spark-3.5-starter.jar
APP_JAR=${APP_DIR}/starter/${APP_JAR_NAME}
APP_MAIN="org.apache.seatunnel.core.starter.spark.SparkStarter"

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

if [ $# -eq 0 ]; then
    set -- -h
fi

# Preserve the historical whitespace-separated JAVA_OPTS without pathname expansion.
shell_flags=$-
set -f
java_opts=(${JAVA_OPTS:-})
case "$shell_flags" in
  *f*) ;;
  *) set +f ;;
esac
java_opts+=("-Dseatunnel.spark.starter.jar.name=${APP_JAR_NAME}")
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  java_opts+=("-Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties")
  java_opts+=("-Dseatunnel.logs.path=${APP_DIR}/logs")
  java_opts+=("-Dseatunnel.logs.file_name=seatunnel-spark-3.5-starter")
fi

CLASS_PATH=${APP_DIR}/starter/logging/*:${APP_JAR}

ARGS_FILE=$(mktemp "${TMPDIR:-/tmp}/seatunnel-spark-args.XXXXXXXX")
trap 'rm -f "$ARGS_FILE"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

EXIT_CODE=0
java "${java_opts[@]}" "-Dseatunnel.spark.starter.args-file=${ARGS_FILE}" \
    -cp "${CLASS_PATH}" "${APP_MAIN}" "$@" || EXIT_CODE=$?
if [ "$EXIT_CODE" -eq 234 ]; then
    exit 0
elif [ "$EXIT_CODE" -ne 0 ]; then
    exit "$EXIT_CODE"
fi

spark_args=()
while IFS= read -r -d '' arg; do
    spark_args+=("$arg")
done < "$ARGS_FILE"
if [ "${#spark_args[@]}" -eq 0 ]; then
    echo "Spark starter produced no arguments." >&2
    exit 1
fi
: "${SPARK_HOME:?SPARK_HOME must point to a Spark installation}"
"${SPARK_HOME}/bin/spark-submit" "${spark_args[@]}"
