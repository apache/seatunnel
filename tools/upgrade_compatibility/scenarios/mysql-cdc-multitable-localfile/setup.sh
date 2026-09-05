#!/usr/bin/env bash
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

set -euo pipefail

MYSQL_CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-seatunnel-upgrade-compatibility-mysql}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mysqlpw}"
MYSQL_DRIVER_VERSION="${MYSQL_DRIVER_VERSION:-8.0.32}"
MYSQL_DRIVER_JAR="${RUN_DIR}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar"

docker rm -f "${MYSQL_CONTAINER_NAME}" >/dev/null 2>&1 || true
docker run \
    --detach \
    --name "${MYSQL_CONTAINER_NAME}" \
    --publish "${MYSQL_PORT}:3306" \
    --env MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
    --volume "${SCENARIO_DIR}/mysql.cnf:/etc/mysql/conf.d/seatunnel.cnf:ro" \
    "${MYSQL_IMAGE}" >/dev/null

for _ in $(seq 1 60); do
    if docker exec "${MYSQL_CONTAINER_NAME}" \
        mysqladmin ping -h127.0.0.1 -uroot -p"${MYSQL_ROOT_PASSWORD}" --silent >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

docker exec "${MYSQL_CONTAINER_NAME}" \
    mysqladmin ping -h127.0.0.1 -uroot -p"${MYSQL_ROOT_PASSWORD}" --silent >/dev/null
docker exec -i "${MYSQL_CONTAINER_NAME}" \
    mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" < "${SCENARIO_DIR}/mysql.sql"

"${ROOT_DIR}/mvnw" --batch-mode dependency:copy \
    -Dartifact="com.mysql:mysql-connector-j:${MYSQL_DRIVER_VERSION}" \
    -DoutputDirectory="${RUN_DIR}" \
    -Dmdep.stripVersion=false

install_driver() {
    local dist_dir="$1"

    mkdir -p "${dist_dir}/plugins/MySQL-CDC/lib" "${dist_dir}/lib"
    cp "${MYSQL_DRIVER_JAR}" "${dist_dir}/plugins/MySQL-CDC/lib/"
    cp "${MYSQL_DRIVER_JAR}" "${dist_dir}/lib/"
}

install_driver "${OLD_DIST_DIR}"
install_driver "${CURRENT_DIST_DIR}"
