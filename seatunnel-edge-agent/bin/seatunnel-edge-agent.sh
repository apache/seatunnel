#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_CONF_FILE="${BASE_DIR}/config/agent.yaml"
CONF_FILE="${EDGE_AGENT_CONFIG:-${DEFAULT_CONF_FILE}}"
PID_FILE="${EDGE_AGENT_PID_FILE:-${BASE_DIR}/edge-agent.pid}"
ID_FILE="${EDGE_AGENT_ID_FILE:-${BASE_DIR}/edge-agent.id}"
LOG_FILE="${EDGE_AGENT_LOG_FILE:-${BASE_DIR}/edge-agent.out}"
DEFAULT_LOG_CONFIG_FILE="${BASE_DIR}/config/log4j2.properties"
LOG_CONFIG_FILE="${EDGE_AGENT_LOG_CONFIG:-${DEFAULT_LOG_CONFIG_FILE}}"
APP_LOG_DIR="${EDGE_AGENT_LOG_DIR:-${BASE_DIR}/log}"
APP_LOG_NAME="${EDGE_AGENT_APP_LOG_NAME:-edge-agent.log}"
STARTUP_READY_TIMEOUT_S="${EDGE_AGENT_STARTUP_READY_TIMEOUT_S:-10}"

MAIN_CLASS="org.apache.seatunnel.edge.agent.starter.EdgeAgentStarter"
APP_JAR="${BASE_DIR}/starter/seatunnel-edge-agent-starter.jar"

usage() {
  cat <<EOF
Usage: $(basename "$0") {start|stop|status|db|help}

Commands:
  start   Start edge-agent in the background.
  stop    Stop the process recorded in the PID file (if running).
  status  Report whether the PID file refers to a live process.
  db      SQLite WAL / source-position operations (see: $(basename "$0") db help)
  help    Show this message.

Direct JVM run (without this script): java ... EdgeAgentStarter --help

Environment overrides (optional):
  EDGE_AGENT_CONFIG      Path to agent YAML (default: ${BASE_DIR}/config/agent.yaml)
  EDGE_AGENT_SQLITE_PATH Override SQLite database file for db commands
  EDGE_AGENT_PID_FILE    PID file path (default: ${BASE_DIR}/edge-agent.pid)
  EDGE_AGENT_ID_FILE     Identity file path (default: ${BASE_DIR}/edge-agent.id)
  EDGE_AGENT_LOG_FILE    Startup log file (default: ${BASE_DIR}/edge-agent.out)
  EDGE_AGENT_LOG_CONFIG  log4j2 config path (default: ${BASE_DIR}/config/log4j2.properties)
  EDGE_AGENT_LOG_DIR     Application log directory (default: ${BASE_DIR}/log)
  EDGE_AGENT_APP_LOG_NAME  Application log filename (default: edge-agent.log)
  EDGE_AGENT_STARTUP_READY_TIMEOUT_S  Wait seconds for BOOTSTRAP_READY marker (default: 10)

Layout expected under ${BASE_DIR}:
  starter/seatunnel-edge-agent-starter.jar  main runnable jar
  starter/logging/  logging jars
  config/ configuration directory on classpath
  log/    runtime log directory
EOF
}

write_out() {
  local msg="$*"
  mkdir -p "$(dirname "${LOG_FILE}")" >/dev/null 2>&1 || true
  echo "${msg}" >>"${LOG_FILE}" 2>/dev/null || true
}


fail() {
  write_out "edge-agent: error: $*"
  echo "edge-agent: error: $*" >&2
  exit 1
}

require_java() {
  if ! command -v java >/dev/null 2>&1; then
    fail "'java' not found in PATH; install a JRE/JDK and retry."
  fi
}

require_conf() {
  if [[ ! -f "${CONF_FILE}" ]]; then
    fail "config file not found: ${CONF_FILE} (set EDGE_AGENT_CONFIG or create the file)."
  fi
}

require_main_jar() {
  if [[ ! -f "${APP_JAR}" ]]; then
    fail "main jar not found: ${APP_JAR}"
  fi
}

build_classpath() {
  local cp="${APP_JAR}"
  if [[ -d "${BASE_DIR}/config" ]]; then
    cp="${BASE_DIR}/config:${cp}"
  fi
  local jar
  for jar in "${BASE_DIR}/starter/logging"/*.jar; do
    if [[ ! -f "${jar}" ]]; then
      continue
    fi
    cp="${cp}:${jar}"
  done
  for jar in "${BASE_DIR}/lib"/*.jar; do
    if [[ ! -f "${jar}" ]]; then
      continue
    fi
    cp="${cp}:${jar}"
  done
  echo "${cp}"
}

read_pid() {
  local pid
  if [[ ! -f "${PID_FILE}" ]]; then
    echo ""
    return 0
  fi
  pid="$(tr -d ' \t\r\n' <"${PID_FILE}" || true)"
  # tolerate blank / corrupted pid files
  if [[ -z "${pid}" ]] || [[ ! "${pid}" =~ ^[0-9]+$ ]]; then
    echo ""
    return 0
  fi
  echo "${pid}"
}

is_running() {
  local pid
  pid="$(read_pid)"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  if kill -0 "${pid}" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

cleanup_stale_pid() {
  local pid
  pid="$(read_pid)"
  if [[ -n "${pid}" ]] && ! kill -0 "${pid}" >/dev/null 2>&1; then
    rm -f "${PID_FILE}"
  fi
}

start() {
  cleanup_stale_pid
  if is_running; then
    echo "edge-agent already running (pid $(read_pid), pid file ${PID_FILE})."
    exit 0
  fi

  require_java
  require_conf
  require_main_jar

  local cp
  cp="$(build_classpath)"
  mkdir -p "$(dirname "${LOG_FILE}")"
  mkdir -p "${APP_LOG_DIR}"
  write_out "edge-agent: start requested"

  rm -f "${PID_FILE}"
  local app_log_file="${APP_LOG_DIR}/${APP_LOG_NAME}"
  local java_opts=(
    "-Dedge.agent.home=${BASE_DIR}"
    "-Dedge.agent.log.dir=${APP_LOG_DIR}"
    "-Dedge.agent.log.name=${APP_LOG_NAME}"
    "-Dlog4j2.configurationFile=${LOG_CONFIG_FILE}"
  )
  nohup java "${java_opts[@]}" -cp "${cp}" "${MAIN_CLASS}" --config "${CONF_FILE}" >>"${app_log_file}" 2>&1 &
  echo $! >"${PID_FILE}"

  sleep 1
  if ! is_running; then
    rm -f "${PID_FILE}"
    write_out "edge-agent: startup failed; process exited immediately."
    if [[ -f "${app_log_file}" ]]; then
      write_out "edge-agent: startup error details (tail from ${app_log_file}):"
      tail -n 40 "${app_log_file}" >>"${LOG_FILE}" 2>/dev/null || true
    fi
    fail "process exited immediately after start; see ${LOG_FILE} and ${app_log_file} for details."
  fi

  local startup_deadline=$((SECONDS + STARTUP_READY_TIMEOUT_S))
  local marker_found=0
  while [[ ${SECONDS} -lt ${startup_deadline} ]]; do
    if [[ -f "${app_log_file}" ]] && grep -q "BOOTSTRAP_READY" "${app_log_file}"; then
      marker_found=1
      break
    fi
    sleep 1
  done
  if [[ ${marker_found} -ne 1 ]]; then
    write_out "edge-agent: warning: BOOTSTRAP_READY not found within ${STARTUP_READY_TIMEOUT_S}s."
    echo "edge-agent: warning: BOOTSTRAP_READY not found within ${STARTUP_READY_TIMEOUT_S}s; check ${LOG_FILE} and ${app_log_file}." >&2
  fi

  echo "edge-agent started (pid $(read_pid))."
  echo "  log:     ${LOG_FILE}"
  echo "  app-log: ${app_log_file}"
  echo "  pid:     ${PID_FILE}"
  write_out "edge-agent: started pid=$(read_pid) app-log=${app_log_file}"
}

stop_agent() {
  cleanup_stale_pid
  if ! is_running; then
    rm -f "${PID_FILE}"
    echo "edge-agent not running (no live pid in ${PID_FILE})."
    exit 0
  fi

  local pid
  pid="$(read_pid)"
  if kill "${pid}" >/dev/null 2>&1; then
    :
  else
    echo "edge-agent: warning: failed to signal pid ${pid}; removing stale pid file." >&2
    rm -f "${PID_FILE}"
    exit 1
  fi

  local waited=0
  while kill -0 "${pid}" >/dev/null 2>&1 && [[ "${waited}" -lt 30 ]]; do
    sleep 1
    waited=$((waited + 1))
  done

  if kill -0 "${pid}" >/dev/null 2>&1; then
    echo "edge-agent: warning: pid ${pid} still alive after ${waited}s; sending SIGKILL." >&2
    kill -9 "${pid}" >/dev/null 2>&1 || true
  fi

  rm -f "${PID_FILE}"
  echo "edge-agent stopped."
}

db_dispatch() {
  shift
  require_java
  require_main_jar
  local cp
  cp="$(build_classpath)"
  local java_opts=(
    "-Dedge.agent.home=${BASE_DIR}"
    "-Dedge.agent.log.dir=${APP_LOG_DIR}"
    "-Dedge.agent.log.name=${APP_LOG_NAME}"
    "-Dlog4j2.configurationFile=${LOG_CONFIG_FILE}"
  )
  java "${java_opts[@]}" -cp "${cp}" "${MAIN_CLASS}" db "$@"
}

status_agent() {
  cleanup_stale_pid
  if is_running; then
    echo "edge-agent running (pid $(read_pid))."
    echo "  log:     ${LOG_FILE}"
    echo "  app-log: ${APP_LOG_DIR}/${APP_LOG_NAME}"
    echo "  pid:     ${PID_FILE}"
  else
    echo "edge-agent not running."
    if [[ -f "${PID_FILE}" ]]; then
      echo "  note: pid file exists but process is dead: ${PID_FILE}"
    fi
  fi
}

cmd="${1:-}"
case "${cmd}" in
  start) start ;;
  stop) stop_agent ;;
  status) status_agent ;;
  db) db_dispatch "$@" ;;
  help | -h | --help) usage ;;
  "")
    usage
    exit 1
    ;;
  *)
    echo "edge-agent: unknown command '${cmd}'." >&2
    usage >&2
    exit 1
    ;;
esac
