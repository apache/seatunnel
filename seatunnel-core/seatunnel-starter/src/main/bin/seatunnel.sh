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
SEATUNNEL_HOME=${APP_DIR}
CONF_DIR=${APP_DIR}/config
APP_JAR=${APP_DIR}/starter/seatunnel-starter.jar
APP_MAIN="org.apache.seatunnel.core.starter.seatunnel.SeaTunnelClient"

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

if [ $# == 0 ]
then
    args="-h"
else
    args=$@
fi

set +u
# SeaTunnel Engine Config
if [ -z $HAZELCAST_CLIENT_CONFIG ]; then
    HAZELCAST_CLIENT_CONFIG=${CONF_DIR}/hazelcast-client.yaml
fi

if [ -z $HAZELCAST_CONFIG ]; then
  HAZELCAST_CONFIG=${CONF_DIR}/hazelcast.yaml
fi

if [ -z $SEATUNNEL_CONFIG ]; then
    SEATUNNEL_CONFIG=${CONF_DIR}/seatunnel.yaml
fi

if test ${JvmOption} ;then
    JAVA_OPTS="${JAVA_OPTS} ${JvmOption}"
fi

JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.client.config=${HAZELCAST_CLIENT_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.config=${SEATUNNEL_CONFIG}"
JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.config=${HAZELCAST_CONFIG}"

# Client Debug Config
# Usage instructions:
# If you need to debug your code in cluster mode, please enable this configuration option and listen to the specified
# port in your IDE. After that, you can happily debug your code.
# JAVA_OPTS="${JAVA_OPTS} -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=5000,suspend=n"

# Log4j2 Config
JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.isThreadContextMapInheritable=true"
if [ -e "${CONF_DIR}/log4j2_client.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=${CONF_DIR}/log4j2_client.properties"
  JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.path=${APP_DIR}/logs"
  if [[ $args == *" -m local"* || $args == *" --master local"* || $args == *" -e local"* || $args == *" --deploy-mode local"* ]]; then
    ntime=$(echo `date "+%N"`|sed -r 's/^0+//')
    JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-starter-client-$((`date '+%s'`*1000+$ntime/1000000))"
  else
      JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-starter-client"
  fi
fi

CLASS_PATH=${APP_DIR}/lib/*:${APP_JAR}

while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ ! $line == \#* ]]; then
        JAVA_OPTS="$JAVA_OPTS $line"
    fi
done < ${APP_DIR}/config/jvm_client_options

# Parse JvmOption from command line, it should be parsed after jvm_client_options
for i in "$@"
do
  if [[ "${i}" == *"JvmOption"* ]]; then
    JVM_OPTION="${i}"
    JAVA_OPTS="${JAVA_OPTS} ${JVM_OPTION#*=}"
    break
  fi
done

java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}
