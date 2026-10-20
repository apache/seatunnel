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
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

PRG_DIR=$(dirname "$PRG")
APP_DIR=$(cd "$PRG_DIR/.." >/dev/null; pwd)
CONF_DIR=${APP_DIR}/config
SESSION_MASTER_MAIN="org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesSessionMasterCli"

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
  . "${CONF_DIR}/seatunnel-env.sh"
fi

if [ -z "${SEATUNNEL_HOME:-}" ]; then
  SEATUNNEL_HOME=${APP_DIR}
fi

SEATUNNEL_CONFIG=${SEATUNNEL_CONFIG:-${CONF_DIR}/seatunnel.yaml}
HAZELCAST_CONFIG=${HAZELCAST_CONFIG:-${CONF_DIR}/hazelcast-master.yaml}
SEATUNNEL_SESSION_CONFIG=${SEATUNNEL_SESSION_CONFIG:-${CONF_DIR}/session.yaml}

if [ ! -f "${SEATUNNEL_CONFIG}" ]; then
  echo "Error: File ${SEATUNNEL_CONFIG} does not exist." >&2
  exit 1
fi
if [ ! -f "${HAZELCAST_CONFIG}" ]; then
  echo "Error: File ${HAZELCAST_CONFIG} does not exist." >&2
  exit 1
fi
if [ ! -f "${SEATUNNEL_SESSION_CONFIG}" ]; then
  echo "Error: File ${SEATUNNEL_SESSION_CONFIG} does not exist." >&2
  exit 1
fi

JAVA_OPTS="${JAVA_OPTS:-} -Dseatunnel.home=${SEATUNNEL_HOME} -DSEATUNNEL_HOME=${SEATUNNEL_HOME}"
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.config=${SEATUNNEL_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.config=${HAZELCAST_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.session.config=${SEATUNNEL_SESSION_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.path=${APP_DIR}/logs"
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-session-master"
JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.isThreadContextMapInheritable=true -DAsyncLogger.ThreadNameStrategy=UNCACHED"

if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
fi

if [ -f "${CONF_DIR}/jvm_master_options" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    if [[ ! "$line" =~ ^# ]]; then
      JAVA_OPTS="${JAVA_OPTS} ${line}"
    fi
  done < "${CONF_DIR}/jvm_master_options"
fi

for argument in "$@"; do
  if [[ "${argument}" == *"JvmOption"* ]]; then
    JAVA_OPTS="${JAVA_OPTS} ${argument#*=}"
  fi
done

for module_flag in \
  "--add-opens=java.base/java.lang=ALL-UNNAMED" \
  "--add-opens=java.base/java.net=ALL-UNNAMED" \
  "--add-opens=java.base/java.nio=ALL-UNNAMED" \
  "--add-opens=java.base/java.util=ALL-UNNAMED" \
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED"; do
  case " ${JAVA_OPTS} " in
    *" ${module_flag} "*) ;;
    *) JAVA_OPTS="${JAVA_OPTS} ${module_flag}" ;;
  esac
done

CLASS_PATH="${APP_DIR}/lib/*:${APP_DIR}/starter/seatunnel-starter.jar:${APP_DIR}/resource-managers/kubernetes/*"

exec java ${JAVA_OPTS} -cp "${CLASS_PATH}" "${SESSION_MASTER_MAIN}" "$@"
