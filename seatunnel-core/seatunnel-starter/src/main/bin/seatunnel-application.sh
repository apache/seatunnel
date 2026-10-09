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

# Application deployment is opt-in and loads only the selected platform SDK.
set -euo pipefail
APPLICATION_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SEATUNNEL_HOME="${APPLICATION_HOME}"

CONF_DIR="${APPLICATION_HOME}/config"
APP_JAR="${APPLICATION_HOME}/starter/seatunnel-starter.jar"
APP_MAIN="org.apache.seatunnel.core.starter.seatunnel.SeaTunnelApplication"

if [[ -f "${CONF_DIR}/seatunnel-env.sh" ]]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

# Parse the deployment target without modifying the original arguments.
application_target=""
application_previous=""
for application_argument in "$@"; do
    case "${application_previous}" in
        --target|-t) application_target="${application_argument}" ;;
    esac
    case "${application_argument}" in
        --target=*|-t=*) application_target="${application_argument#*=}" ;;
    esac
    application_previous="${application_argument}"
done

case "${application_target}" in
    [Yy][Aa][Rr][Nn]) application_target="yarn" ;;
    [Kk][Uu][Bb][Ee][Rr][Nn][Ee][Tt][Ee][Ss]) application_target="kubernetes" ;;
    "") ;;
    *) echo "Unsupported --target: ${application_target}" >&2 exit 1 ;;
esac

# Load only the SDK required by the selected deployment platform.
CLASS_PATH="${APP_JAR}:${APPLICATION_HOME}/starter/logging/*:${APPLICATION_HOME}/lib/*:${CONF_DIR}"

if [[ -n "${application_target}" ]]; then
    CLASS_PATH="${CLASS_PATH}:${APPLICATION_HOME}/resource-managers/${application_target}/*"
fi

if [[ -n "${HADOOP_CONF_DIR:-}" && "${application_target}" == "yarn" ]]; then
    CLASS_PATH="${CLASS_PATH}:${HADOOP_CONF_DIR}"
fi

# Configure logging consistently with the SeaTunnel client launcher.
JAVA_OPTS="${JAVA_OPTS:-} -Dlog4j2.isThreadContextMapInheritable=true"

if [[ -f "${CONF_DIR}/log4j2_client.properties" ]]; then
    JAVA_OPTS="${JAVA_OPTS} -Dhazelcast.logging.type=log4j2"
    JAVA_OPTS="${JAVA_OPTS} -Dlog4j2.configurationFile=${CONF_DIR}/log4j2_client.properties"
    JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.path=${APPLICATION_HOME}/logs"
    JAVA_OPTS="${JAVA_OPTS} -Dseatunnel.logs.file_name=seatunnel-application"
fi

# Load additional JVM options from the distribution configuration.
JVM_OPTIONS_FILE="${CONF_DIR}/jvm_client_options"
if [[ -f "${JVM_OPTIONS_FILE}" ]]; then
    while IFS= read -r line || [[ -n "${line}" ]]; do
        [[ "${line}" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line//[[:space:]]/}" ]] && continue
        JAVA_OPTS="${JAVA_OPTS} ${line}"
    done < "${JVM_OPTIONS_FILE}"
fi

# Use JAVA_HOME when configured, otherwise resolve Java from PATH.
if [[ -n "${JAVA_HOME:-}" ]]; then
    APPLICATION_JAVA="${JAVA_HOME}/bin/java"
else
    APPLICATION_JAVA="java"
fi

if ! command -v "${APPLICATION_JAVA}" >/dev/null 2>&1; then
    echo "Error: Java executable not found: ${APPLICATION_JAVA}" >&2
    exit 1
fi

# Fail fast with an actionable message on unsupported JDK versions.
JAVA_MAJOR_VERSION=$(
    "${APPLICATION_JAVA}" -version 2>&1 |
        awk -F '[".]' '/version/ { print ($2 == "1") ? $3 : $2; exit }'
)

if [[ -n "${JAVA_MAJOR_VERSION}" && "${JAVA_MAJOR_VERSION}" -lt 11 ]]; then
    echo "Error: SeaTunnel requires Java 11 or newer, but Java ${JAVA_MAJOR_VERSION} was detected." >&2
    echo "Point JAVA_HOME or PATH at a Java 11+ JDK." >&2
    exit 1
fi

JAVA_OPTS="${JAVA_OPTS:-} -Dlog4j2.isThreadContextMapInheritable=true"

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

# Preserve user-supplied JVM options and pass application arguments unchanged.
exec "${APPLICATION_JAVA}" "${JAVA_OPTS}" \
    -Dseatunnel.home="${APPLICATION_HOME}" \
    -cp "${CLASS_PATH}" \
    "${APP_MAIN}" "$@"