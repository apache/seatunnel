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
APP_DIR=$(cd $(dirname ${0})/../;pwd)
CONF_DIR=${APP_DIR}/config
APP_JAR=${APP_DIR}/lib/seatunnel-core-spark.jar

if [ -f "${CONF_DIR}/seatunnel-env.sh" ]; then
    . "${CONF_DIR}/seatunnel-env.sh"
fi

CMD=$(java -cp ${APP_JAR} org.apache.seatunnel.SparkStarter ${@} | tail -n 1) && EXIT_CODE=$? || EXIT_CODE=$?
if [ ${EXIT_CODE} -eq 234 ]; then
    # print usage
    echo ${CMD}
    exit 0
elif [ ${EXIT_CODE} -eq 0 ]; then
    echo "Execute SeaTunnel Spark Job: ${CMD}"
    eval ${CMD}
else
    echo ${CMD}
    exit ${EXIT_CODE}
fi
