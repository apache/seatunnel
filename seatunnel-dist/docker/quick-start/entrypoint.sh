#!/bin/bash
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
set -eux

log() {
    echo ">>> $@"
}

start_flink() {
  log "Start Flink cluster..."
  ${FLINK_HOME}/bin/start-cluster.sh
}

main() {
  local PARAMS=""
  local ENGINE=""
  local CONFIG=""
  local MASTER="local[2]"
  local DEPLOY_MODE="client"
  while [ $# -ne 0 ]; do
    case "$1" in
      --engine)
        ENGINE=$2
        shift 2
        ;;
      -m|--master)
        MASTER=$2
        shift 2
        ;;
      -e|--deploy-mode)
        DEPLOY_MODE=$2
        shift 2
        ;;
      -c|--config)
        CONFIG=$2
        shift 2
        ;;
      *)
        PARAMS="$PARAMS $1"
        shift
        ;;
    esac
  done
  if [ ${ENGINE} = "spark" ]; then
    if [ ! -d ${SPARK_HOME} ]; then
      log "Spark not deployed!"
      exit 2
    fi
    set -- "-m ${MASTER} -e ${DEPLOY_MODE} ${PARAMS}"
    CONFIG=${CONFIG:-${SEATUNNEL_HOME}/config/spark.batch.conf.template}
    log "Run SeaTunnel job on spark..."
    exec ${SEATUNNEL_HOME}/bin/start-seatunnel-spark.sh --config ${CONFIG} $@
  elif [ ${ENGINE} = "flink" ]; then
    if [ ! -d ${FLINK_HOME} ]; then
      log "Flink not deployed!"
      exit 3
    fi
    set -- "${PARAMS}"
    CONFIG=${CONFIG:-${SEATUNNEL_HOME}/config/flink.streaming.conf.template}
    start_flink
    log "Run SeaTunnel job on flink..."
    exec ${SEATUNNEL_HOME}/bin/start-seatunnel-flink.sh --config ${CONFIG} $@
  else
    log "Engine not support: ${ENGINE}"
    exit 1
  fi
}

main $@
