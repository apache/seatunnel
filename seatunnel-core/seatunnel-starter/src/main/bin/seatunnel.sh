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

if [ $# == 0 ]; then
    set -- -h
fi
args=("$@")
args_str=" $* "

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
  if [[ "$args_str" == *" -m local "* || "$args_str" == *" --master local "* || "$args_str" == *" -e local "* || "$args_str" == *" --deploy-mode local "* ]]; then
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

# SeaTunnel requires Java 11 or newer. Fail fast with an actionable message instead of letting a
# JDK 8 launcher abort on the JDK 9+ module flags below with a cryptic "Unrecognized option" error.
JAVA_MAJOR_VERSION=$(java -version 2>&1 | awk -F '[".]' '/version/ {print ($2 == "1") ? $3 : $2; exit}')
if [[ -n "$JAVA_MAJOR_VERSION" && "$JAVA_MAJOR_VERSION" -lt 11 ]]; then
  echo "Error: SeaTunnel requires Java 11 or newer, but Java ${JAVA_MAJOR_VERSION} was detected. Point JAVA_HOME/PATH at a Java 11+ JDK." >&2
  exit 1
fi

# These JDK module flags are mandatory on Java 11+: Hazelcast needs reflective access to JDK
# internals and the Kerberos krb5.conf reload needs the jgss export. They are appended here, not
# only shipped in the config/jvm_*_options templates, so an in-place upgrade that preserves an old
# config directory (mounted Docker volume, K8s ConfigMap) cannot silently drop them. Appending a
# flag twice is harmless, so config files that already carry them stay compatible.
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

# Ensure HeapDumpPath directory exists to avoid OOM dump failures.
HEAP_DUMP_PATH=""
for opt in $JAVA_OPTS; do
  if [[ "$opt" == -XX:HeapDumpPath=* ]]; then
    HEAP_DUMP_PATH="${opt#-XX:HeapDumpPath=}"
  fi
done
if [[ -n "$HEAP_DUMP_PATH" ]]; then
  HEAP_DUMP_DIR="$HEAP_DUMP_PATH"
  if [[ "$HEAP_DUMP_PATH" == */ ]]; then
    HEAP_DUMP_DIR="${HEAP_DUMP_PATH%/}"
  elif [[ "$HEAP_DUMP_PATH" == *.hprof || "$HEAP_DUMP_PATH" == *.phd ]]; then
    HEAP_DUMP_DIR="$(dirname "$HEAP_DUMP_PATH")"
  elif [[ -e "$HEAP_DUMP_PATH" && ! -d "$HEAP_DUMP_PATH" ]]; then
    HEAP_DUMP_DIR="$(dirname "$HEAP_DUMP_PATH")"
  elif [[ "${HEAP_DUMP_PATH##*/}" == *.* ]]; then
    HEAP_DUMP_DIR="$(dirname "$HEAP_DUMP_PATH")"
  fi
  if [[ -n "$HEAP_DUMP_DIR" && ! -d "$HEAP_DUMP_DIR" ]]; then
    mkdir -p "$HEAP_DUMP_DIR"
  fi
fi

# Ensure Xloggc directory exists to avoid GC logging failures.
GC_LOG_PATH=""
for opt in $JAVA_OPTS; do
  if [[ "$opt" == -Xloggc:* ]]; then
    GC_LOG_PATH="${opt#-Xloggc:}"
  fi
done
if [[ -n "$GC_LOG_PATH" ]]; then
  GC_LOG_DIR="$(dirname "$GC_LOG_PATH")"
  if [[ -n "$GC_LOG_DIR" && ! -d "$GC_LOG_DIR" ]]; then
    mkdir -p "$GC_LOG_DIR"
  fi
fi

# log4j-api writes this notice to stdout on every JVM newer than Java 8, because
# sun.reflect.Reflection.getCallerClass no longer exists there. It corrupts the machine readable
# output of commands that emit JSON, such as `-j <jobId>`, so drop just that line.
#
# set -e is active from the top of the script, and grep exits 1 when it emits no lines at all, so
# the pipeline has to run with errexit off or a command with empty stdout would abort the script
# before the exit below. PIPESTATUS[0] then reports java's own status rather than grep's.
# --line-buffered keeps job output streaming when stdout is redirected to a file.
set +e
java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} "${args[@]}" \
  | grep -v --line-buffered '^WARNING: sun\.reflect\.Reflection\.getCallerClass is not supported'
JAVA_EXIT_CODE=${PIPESTATUS[0]}
set -e
exit ${JAVA_EXIT_CODE}
