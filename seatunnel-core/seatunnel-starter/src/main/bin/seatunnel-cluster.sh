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
# resolve links - $0 may be a softlink
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
APP_JAR=${APP_DIR}/starter/seatunnel-starter.jar
APP_MAIN="org.apache.seatunnel.core.starter.seatunnel.SeaTunnelServer"
MASTER_OUT="${APP_DIR}/logs/seatunnel-engine-master.out"
WORKER_OUT="${APP_DIR}/logs/seatunnel-engine-worker.out"
OUT="${APP_DIR}/logs/seatunnel-server.out"
HELP=false
NODE_ROLE="master_and_worker"

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

if [ $# == 0 ]
then
    args=""
else
    args=$@
fi

set +u

if [ -z $SEATUNNEL_CONFIG ]; then
    SEATUNNEL_CONFIG=${CONF_DIR}/seatunnel.yaml
fi

if test ${JvmOption} ;then
    JAVA_OPTS="${JAVA_OPTS} ${JvmOption}"
fi

for i in "$@"
do
  if [[ "${i}" == *"JvmOption"* ]]; then
    :
  elif [[ "${i}" == "-d" || "${i}" == "--daemon" ]]; then
    DAEMON=true
  elif [[ "${i}" == "-r" || "${i}" == "--role" ]]; then
    ROLE_FLAG=true
  elif [[ "${ROLE_FLAG}" == true ]]; then
    NODE_ROLE="${i}"
    ROLE_FLAG=false
  elif [[ "${i}" == "-h" || "${i}" == "--help" ]]; then
    HELP=true
  fi
done

# Log4j2 Config
JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.isThreadContextMapInheritable=true -DAsyncLogger.ThreadNameStrategy=UNCACHED"
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.path=${APP_DIR}/logs"
fi

if [ "$NODE_ROLE" = "master" ]; then
  OUT=$MASTER_OUT
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-engine-master"
  while IFS= read -r line || [ -n "$line" ]; do
      if [[ ! "$line" =~ ^# ]]; then
          JAVA_OPTS="$JAVA_OPTS $line"
      fi
  done < ${APP_DIR}/config/jvm_master_options
  # SeaTunnel Engine Config
  if [ -z $HAZELCAST_CONFIG ]; then
    HAZELCAST_CONFIG=${CONF_DIR}/hazelcast-master.yaml
  fi
elif [ "$NODE_ROLE" = "worker" ]; then
  OUT=$WORKER_OUT
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-engine-worker"
  while IFS= read -r line || [ -n "$line" ]; do
      if [[ ! "$line" =~ ^# ]]; then
          JAVA_OPTS="$JAVA_OPTS $line"
      fi
  done < ${APP_DIR}/config/jvm_worker_options
  if [ -z $HAZELCAST_CONFIG ]; then
    HAZELCAST_CONFIG=${CONF_DIR}/hazelcast-worker.yaml
  fi
elif [ "$NODE_ROLE" = "master_and_worker" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-engine-server"
  while IFS= read -r line || [ -n "$line" ]; do
      if [[ ! "$line" =~ ^# ]]; then
          JAVA_OPTS="$JAVA_OPTS $line"
      fi
  done < ${APP_DIR}/config/jvm_options
  if [ -z $HAZELCAST_CONFIG ]; then
    HAZELCAST_CONFIG=${CONF_DIR}/hazelcast.yaml
  fi
else
  echo "Unknown node role: $NODE_ROLE"
  exit 1
fi

if [ ! -f "$HAZELCAST_CONFIG" ]; then
    echo "Error: File $HAZELCAST_CONFIG does not exist."
    exit 1
fi
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.config=${SEATUNNEL_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.config=${HAZELCAST_CONFIG}"
# Server Debug Config
# Usage instructions:
# If you need to debug your code in cluster mode, please enable this configuration option and listen to the specified
# port in your IDE. After that, you can happily debug your code.
# JAVA_OPTS="${JAVA_OPTS} -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5001,suspend=n"

# Parse JvmOption from command line, it should be parsed after jvm_options
for i in "$@"
do
  if [[ "${i}" == *"JvmOption"* ]]; then
    JVM_OPTION="${i}"
    JAVA_OPTS="${JAVA_OPTS} ${JVM_OPTION#*=}"
  fi
done

CLASS_PATH=${APP_DIR}/lib/*:${APP_JAR}

echo "start ${NODE_ROLE} node"

if [[ $DAEMON == true && $HELP == false ]]; then
  if [[ ! -d ${APP_DIR}/logs ]]; then
    mkdir -p ${APP_DIR}/logs
  fi
  touch $OUT
  nohup java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args} > "$OUT" 200<&- 2>&1 < /dev/null &
  else
  java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}
fi

