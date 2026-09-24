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
if [[ -f "${APPLICATION_HOME}/config/seatunnel-env.sh" ]]; then
    . "${APPLICATION_HOME}/config/seatunnel-env.sh"
fi
application_target=""
application_previous=""
for application_argument in "$@"; do
    if [[ "${application_previous}" == "--target" ]]; then
        application_target="${application_argument}"
    fi
    if [[ "${application_argument}" == --target=* ]]; then
        application_target="${application_argument#--target=}"
    fi
    application_previous="${application_argument}"
done
case "${application_target}" in
    yarn|kubernetes|"") ;;
    *) echo "Unsupported --target: ${application_target}" >&2; exit 1 ;;
esac
application_classpath="${APPLICATION_HOME}/starter/seatunnel-starter.jar:${APPLICATION_HOME}/starter/logging/*:${APPLICATION_HOME}/lib/*:${APPLICATION_HOME}/config"
if [[ -n "${application_target}" ]]; then
    application_classpath="${application_classpath}:${APPLICATION_HOME}/resource-managers/${application_target}/*"
fi
if [[ -n "${HADOOP_CONF_DIR:-}" && "${application_target}" == yarn ]]; then
    application_classpath="${application_classpath}:${HADOOP_CONF_DIR}"
fi
application_java="java"
if [[ -n "${JAVA_HOME:-}" ]]; then
    application_java="${JAVA_HOME}/bin/java"
fi
exec "${application_java}" -Dseatunnel.home="${APPLICATION_HOME}" \
    -cp "${application_classpath}" \
    org.apache.seatunnel.core.starter.seatunnel.SeaTunnelApplication "$@"
