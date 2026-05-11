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
CONF_FILE="${EDGE_AGENT_CONFIG:-${BASE_DIR}/conf/agent.yaml}"
PID_FILE="${EDGE_AGENT_PID_FILE:-${BASE_DIR}/edge-agent.pid}"
LOG_FILE="${EDGE_AGENT_LOG_FILE:-${BASE_DIR}/edge-agent.out}"

MAIN_CLASS="org.apache.seatunnel.edge.agent.EdgeAgentMain"

usage() {
  cat <<EOF
Usage: $(basename "$0") {start|stop|status|help}

Commands:
  start   Start edge-agent in the background.
  stop    Stop the process recorded in the PID file (if running).
  status  Report whether the PID file refers to a live process.
  help    Show this message.

Environment overrides (optional):
  EDGE_AGENT_CONFIG      Path to agent YAML (default: ${BASE_DIR}/conf/agent.yaml)
  EDGE_AGENT_PID_FILE    PID file path (default: ${BASE_DIR}/edge-agent.pid)
  EDGE_AGENT_LOG_FILE    Stdout/stderr log file (default: ${BASE_DIR}/edge-agent.out)

Layout expected under ${BASE_DIR}:
  conf/   configuration directory on classpath
  lib/    dependency jars (*.jar)
EOF
}

fail() {
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

require_lib_jars() {
  local jar
  local found=0
  for jar in "${BASE_DIR}/lib"/*.jar; do
    if [[ -f "${jar}" ]]; then
      found=1
      break
    fi
  done
  if [[ "${found}" -eq 0 ]]; then
    fail "no jars under ${BASE_DIR}/lib; build or copy runtime jars before starting."
  fi
}

build_classpath() {
  local cp="${BASE_DIR}/conf"
  local first=1
  local jar
  for jar in "${BASE_DIR}/lib"/*.jar; do
    if [[ ! -f "${jar}" ]]; then
      continue
    fi
    if [[ "${first}" -eq 1 ]]; then
      cp="${cp}:${jar}"
      first=0
    else
      cp="${cp}:${jar}"
    fi
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
  require_lib_jars

  local cp
  cp="$(build_classpath)"

  rm -f "${PID_FILE}"
  # shellcheck disable=SC2086
  nohup java -cp "${cp}" "${MAIN_CLASS}" --config "${CONF_FILE}" >>"${LOG_FILE}" 2>&1 &
  echo $! >"${PID_FILE}"

  sleep 1
  if ! is_running; then
    rm -f "${PID_FILE}"
    fail "process exited immediately after start; see ${LOG_FILE} for details."
  fi

  echo "edge-agent started (pid $(read_pid))."
  echo "  config: ${CONF_FILE}"
  echo "  log:    ${LOG_FILE}"
  echo "  pid:    ${PID_FILE}"
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

status_agent() {
  cleanup_stale_pid
  if is_running; then
    echo "edge-agent running (pid $(read_pid))."
    echo "  config: ${CONF_FILE}"
    echo "  log:    ${LOG_FILE}"
    echo "  pid:    ${PID_FILE}"
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
