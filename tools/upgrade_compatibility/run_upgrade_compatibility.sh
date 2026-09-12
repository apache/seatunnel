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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." >/dev/null; pwd)"
OLD_SEATUNNEL_VERSION="${OLD_SEATUNNEL_VERSION:-2.3.13}"
SCENARIO="${SCENARIO:-generic-fake-localfile}"
WORK_DIR="${WORK_DIR:-${ROOT_DIR}/target/upgrade-compatibility}"
CURRENT_DIST_ARCHIVE="${CURRENT_DIST_ARCHIVE:-}"
JOB_ID="${JOB_ID:-11239001}"
STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-90}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-180}"
SAVEPOINT_DELAY_SECONDS="${SAVEPOINT_DELAY_SECONDS:-15}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

SCENARIO_DIR="${ROOT_DIR}/tools/upgrade_compatibility/scenarios/${SCENARIO}"
OLD_ARCHIVE="${WORK_DIR}/downloads/apache-seatunnel-${OLD_SEATUNNEL_VERSION}-bin.tar.gz"
OLD_DIST_DIR="${WORK_DIR}/old"
CURRENT_DIST_DIR="${WORK_DIR}/current"
RUN_DIR="${WORK_DIR}/runs/${SCENARIO}"
CHECKPOINT_DIR="${RUN_DIR}/checkpoint"
SINK_DIR="${RUN_DIR}/sink"

OLD_SERVER_PID=""
CURRENT_SERVER_PID=""
OLD_JOB_PID=""
CURRENT_RESTORE_PID=""
SCENARIO_SETUP_STARTED="false"

stage() {
    local message="$1"
    if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        echo "::group::${message}"
    fi
    echo "==> ${message}"
}

end_stage() {
    if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        echo "::endgroup::"
    fi
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

cleanup() {
    stop_process "${CURRENT_RESTORE_PID}"
    stop_process "${OLD_JOB_PID}"
    stop_process "${OLD_SERVER_PID}"
    stop_process "${CURRENT_SERVER_PID}"
    if [ "${SCENARIO_SETUP_STARTED}" = "true" ]; then
        run_scenario_hook "teardown.sh" || true
    fi
}

stop_process() {
    local pid="$1"
    local child_pid

    [ -n "${pid}" ] || return 0

    while IFS= read -r child_pid; do
        stop_process "${child_pid}"
    done < <(pgrep -P "${pid}" 2>/dev/null || true)

    if kill -0 "${pid}" >/dev/null 2>&1; then
        kill "${pid}" >/dev/null 2>&1 || true
        wait "${pid}" >/dev/null 2>&1 || true
    fi
}

trap cleanup EXIT

require_file() {
    local path="$1"
    [ -f "${path}" ] || fail "Missing required file: ${path}"
}

download_old_distribution() {
    stage "Download SeaTunnel ${OLD_SEATUNNEL_VERSION} distribution"
    mkdir -p "$(dirname "${OLD_ARCHIVE}")"
    if [ -f "${OLD_ARCHIVE}" ]; then
        echo "Using cached archive ${OLD_ARCHIVE}"
        end_stage
        return
    fi

    local file_name="apache-seatunnel-${OLD_SEATUNNEL_VERSION}-bin.tar.gz"
    local urls=(
        "https://downloads.apache.org/seatunnel/${OLD_SEATUNNEL_VERSION}/${file_name}"
        "https://archive.apache.org/dist/seatunnel/${OLD_SEATUNNEL_VERSION}/${file_name}"
    )

    local url
    for url in "${urls[@]}"; do
        echo "Trying ${url}"
        if curl --fail --location --retry 3 --retry-delay 5 --output "${OLD_ARCHIVE}.tmp" "${url}"; then
            mv "${OLD_ARCHIVE}.tmp" "${OLD_ARCHIVE}"
            end_stage
            return
        fi
        rm -f "${OLD_ARCHIVE}.tmp"
    done

    fail "Could not download ${file_name}"
}

find_current_distribution() {
    if [ -n "${CURRENT_DIST_ARCHIVE}" ]; then
        require_file "${CURRENT_DIST_ARCHIVE}"
        echo "${CURRENT_DIST_ARCHIVE}"
        return
    fi

    local -a archives=()
    local archive
    while IFS= read -r archive; do
        archives+=("${archive}")
    done < <(
        find "${ROOT_DIR}/seatunnel-dist/target" -maxdepth 1 \
            -name "apache-seatunnel-*-bin.tar.gz" \
            ! -name "apache-seatunnel-edge-agent-*-bin.tar.gz" \
            ! -name "*-src.tar.gz" \
            | sort
    )

    [ "${#archives[@]}" -gt 0 ] \
        || fail "No current dev distribution found under seatunnel-dist/target"
    [ "${#archives[@]}" -eq 1 ] \
        || fail "Multiple current dev distributions found: ${archives[*]}"
    echo "${archives[0]}"
}

extract_distribution() {
    local archive="$1"
    local target_dir="$2"

    rm -rf "${target_dir}"
    mkdir -p "${target_dir}"
    tar -xzf "${archive}" -C "${target_dir}" --strip-components=1
    require_file "${target_dir}/bin/seatunnel.sh"
    require_file "${target_dir}/bin/seatunnel-cluster.sh"
    chmod +x "${target_dir}/bin/"*.sh
}

render_template() {
    local source="$1"
    local target="$2"

    sed \
        -e "s#__CHECKPOINT_DIR__#${CHECKPOINT_DIR}#g" \
        -e "s#__SINK_DIR__#${SINK_DIR}#g" \
        -e "s#__MYSQL_HOST__#${MYSQL_HOST}#g" \
        -e "s#__MYSQL_PORT__#${MYSQL_PORT}#g" \
        "${source}" > "${target}"
}

run_scenario_hook() {
    local hook_name="$1"
    local hook_path="${SCENARIO_DIR}/${hook_name}"

    if [ ! -f "${hook_path}" ]; then
        return 0
    fi
    [ -x "${hook_path}" ] || fail "Scenario hook is not executable: ${hook_path}"

    SCENARIO="${SCENARIO}" \
        SCENARIO_DIR="${SCENARIO_DIR}" \
        ROOT_DIR="${ROOT_DIR}" \
        RUN_DIR="${RUN_DIR}" \
        CHECKPOINT_DIR="${CHECKPOINT_DIR}" \
        SINK_DIR="${SINK_DIR}" \
        OLD_DIST_DIR="${OLD_DIST_DIR}" \
        CURRENT_DIST_DIR="${CURRENT_DIST_DIR}" \
        OLD_SEATUNNEL_VERSION="${OLD_SEATUNNEL_VERSION}" \
        MYSQL_HOST="${MYSQL_HOST}" \
        MYSQL_PORT="${MYSQL_PORT}" \
        "${hook_path}"
}

is_endless_job() {
    [ -f "${SCENARIO_DIR}/endless" ]
}

prepare_distribution() {
    local dist_dir="$1"

    render_template "${SCENARIO_DIR}/seatunnel.yaml" "${dist_dir}/config/seatunnel.yaml"
    render_template "${SCENARIO_DIR}/job.conf" "${dist_dir}/config/upgrade-compatibility-job.conf"
    render_template "${SCENARIO_DIR}/assert.conf" "${dist_dir}/config/upgrade-compatibility-assert.conf"
}

install_old_release_connectors() {
    stage "Install old-release scenario connectors"
    require_file "${SCENARIO_DIR}/plugin_config"
    cp "${SCENARIO_DIR}/plugin_config" "${OLD_DIST_DIR}/config/plugin_config"
    mkdir -p "${OLD_DIST_DIR}/connectors"

    while IFS= read -r connector || [ -n "${connector}" ]; do
        case "${connector}" in
            ""|\#*) continue ;;
        esac
        if ls "${OLD_DIST_DIR}/connectors/${connector}-"*.jar >/dev/null 2>&1; then
            echo "Already present: ${connector}"
            continue
        fi
        echo "Installing ${connector}:${OLD_SEATUNNEL_VERSION}"
        "${ROOT_DIR}/mvnw" --batch-mode dependency:copy \
            -Dartifact="org.apache.seatunnel:${connector}:${OLD_SEATUNNEL_VERSION}" \
            -DoutputDirectory="${OLD_DIST_DIR}/connectors" \
            -Dmdep.stripVersion=false
    done < "${SCENARIO_DIR}/plugin_config"
    end_stage
}

wait_for_log() {
    local log_file="$1"
    local pattern="$2"
    local timeout_seconds="$3"
    local elapsed=0

    until grep -q "${pattern}" "${log_file}" >/dev/null 2>&1; do
        if [ "${elapsed}" -ge "${timeout_seconds}" ]; then
            echo "Timed out waiting for pattern '${pattern}' in ${log_file}" >&2
            tail -n 200 "${log_file}" >&2 || true
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
}

run_with_timeout() {
    local timeout_seconds="$1"
    shift

    "$@" &
    local pid="$!"
    local timeout_marker="${WORK_DIR}/run-${pid}.timeout"
    local watcher_pid
    local status=0

    rm -f "${timeout_marker}"
    (
        sleep "${timeout_seconds}"
        if kill -0 "${pid}" >/dev/null 2>&1; then
            touch "${timeout_marker}"
            kill "${pid}" >/dev/null 2>&1 || true
        fi
    ) &
    watcher_pid="$!"

    wait "${pid}" || status="$?"
    kill "${watcher_pid}" >/dev/null 2>&1 || true
    wait "${watcher_pid}" >/dev/null 2>&1 || true

    if [ -f "${timeout_marker}" ]; then
        rm -f "${timeout_marker}"
        return 124
    fi
    return "${status}"
}

wait_for_process() {
    local pid="$1"
    local timeout_seconds="$2"
    local log_file="$3"
    local timeout_marker="${log_file}.timeout"
    local watcher_pid
    local status=0

    rm -f "${timeout_marker}"
    (
        sleep "${timeout_seconds}"
        if kill -0 "${pid}" >/dev/null 2>&1; then
            touch "${timeout_marker}"
            kill "${pid}" >/dev/null 2>&1 || true
        fi
    ) &
    watcher_pid="$!"

    wait "${pid}" || status="$?"
    kill "${watcher_pid}" >/dev/null 2>&1 || true
    wait "${watcher_pid}" >/dev/null 2>&1 || true

    if [ -f "${timeout_marker}" ]; then
        rm -f "${timeout_marker}"
        echo "Timed out waiting for process ${pid}" >&2
        tail -n 200 "${log_file}" >&2 || true
        return 124
    fi
    if [ "${status}" -ne 0 ]; then
        tail -n 200 "${log_file}" >&2 || true
        return "${status}"
    fi
    return 0
}

wait_for_process_to_stop() {
    local pid="$1"
    local timeout_seconds="$2"
    local log_file="$3"
    local timeout_marker="${log_file}.timeout"
    local watcher_pid

    rm -f "${timeout_marker}"
    (
        sleep "${timeout_seconds}"
        if kill -0 "${pid}" >/dev/null 2>&1; then
            touch "${timeout_marker}"
            kill "${pid}" >/dev/null 2>&1 || true
        fi
    ) &
    watcher_pid="$!"

    wait "${pid}" >/dev/null 2>&1 || true
    kill "${watcher_pid}" >/dev/null 2>&1 || true
    wait "${watcher_pid}" >/dev/null 2>&1 || true

    if [ -f "${timeout_marker}" ]; then
        rm -f "${timeout_marker}"
        echo "Timed out waiting for process ${pid} to stop" >&2
        tail -n 200 "${log_file}" >&2 || true
        return 124
    fi
    return 0
}

start_cluster() {
    local dist_dir="$1"
    local log_file="$2"
    local engine_log="${dist_dir}/logs/seatunnel-engine-server.log"

    mkdir -p "$(dirname "${log_file}")"
    rm -f "${engine_log}"
    SEATUNNEL_CONFIG="${dist_dir}/config/seatunnel.yaml" \
        "${dist_dir}/bin/seatunnel-cluster.sh" > "${log_file}" 2>&1 &
    local pid="$!"
    if ! wait_for_log \
        "${engine_log}" "received new worker register" "${STARTUP_TIMEOUT_SECONDS}"; then
        tail -n 200 "${log_file}" >&2 || true
        stop_process "${pid}"
        return 1
    fi
    echo "${pid}"
}

start_old_job() {
    local dist_dir="$1"
    local log_file="$2"

    mkdir -p "$(dirname "${log_file}")"
    "${dist_dir}/bin/seatunnel.sh" \
        --config "${dist_dir}/config/upgrade-compatibility-job.conf" \
        --name "upgrade-compatibility-${SCENARIO}" \
        --set-job-id "${JOB_ID}" \
        > "${log_file}" 2>&1 &
    echo "$!"
}

savepoint_job() {
    local dist_dir="$1"
    local log_file="$2"

    "${dist_dir}/bin/seatunnel.sh" -s "${JOB_ID}" > "${log_file}" 2>&1
}

cancel_job() {
    local dist_dir="$1"
    local log_file="$2"

    "${dist_dir}/bin/seatunnel.sh" --cancel "${JOB_ID}" > "${log_file}" 2>&1
}

restore_current_job() {
    local dist_dir="$1"
    local log_file="$2"

    run_with_timeout "${JOB_TIMEOUT_SECONDS}" \
        "${dist_dir}/bin/seatunnel.sh" \
        --config "${dist_dir}/config/upgrade-compatibility-job.conf" \
        --name "upgrade-compatibility-${SCENARIO}-restore" \
        -r "${JOB_ID}" \
        > "${log_file}" 2>&1
}

start_current_restore_job() {
    local dist_dir="$1"
    local log_file="$2"

    mkdir -p "$(dirname "${log_file}")"
    "${dist_dir}/bin/seatunnel.sh" \
        --config "${dist_dir}/config/upgrade-compatibility-job.conf" \
        --name "upgrade-compatibility-${SCENARIO}-restore" \
        -r "${JOB_ID}" \
        > "${log_file}" 2>&1 &
    echo "$!"
}

assert_output() {
    local dist_dir="$1"
    local log_file="$2"

    run_with_timeout "${JOB_TIMEOUT_SECONDS}" \
        "${dist_dir}/bin/seatunnel.sh" \
        --config "${dist_dir}/config/upgrade-compatibility-assert.conf" \
        --name "upgrade-compatibility-${SCENARIO}-assert" \
        > "${log_file}" 2>&1
}

assert_output_with_retry() {
    local dist_dir="$1"
    local log_file="$2"
    local timeout_seconds="$3"
    local elapsed=0

    until assert_output "${dist_dir}" "${log_file}"; do
        if [ "${elapsed}" -ge "${timeout_seconds}" ]; then
            tail -n 200 "${log_file}" >&2 || true
            return 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
}

run_scenario() {
    stage "Prepare scenario ${SCENARIO}"
    require_file "${SCENARIO_DIR}/seatunnel.yaml"
    require_file "${SCENARIO_DIR}/job.conf"
    require_file "${SCENARIO_DIR}/assert.conf"

    rm -rf "${RUN_DIR}"
    mkdir -p "${CHECKPOINT_DIR}" "${SINK_DIR}" "${RUN_DIR}/logs"
    prepare_distribution "${OLD_DIST_DIR}"
    prepare_distribution "${CURRENT_DIST_DIR}"
    SCENARIO_SETUP_STARTED="true"
    run_scenario_hook "setup.sh"
    end_stage

    stage "Create savepoint with SeaTunnel ${OLD_SEATUNNEL_VERSION}"
    OLD_SERVER_PID="$(start_cluster "${OLD_DIST_DIR}" "${RUN_DIR}/logs/old-server.log")"
    OLD_JOB_PID="$(start_old_job "${OLD_DIST_DIR}" "${RUN_DIR}/logs/old-job.log")"
    sleep "${SAVEPOINT_DELAY_SECONDS}"
    if ! savepoint_job "${OLD_DIST_DIR}" "${RUN_DIR}/logs/old-savepoint.log"; then
        tail -n 200 "${RUN_DIR}/logs/old-savepoint.log" >&2 || true
        fail "Savepoint failed on SeaTunnel ${OLD_SEATUNNEL_VERSION}"
    fi
    if is_endless_job; then
        cancel_job "${OLD_DIST_DIR}" "${RUN_DIR}/logs/old-cancel.log" || true
        wait_for_process_to_stop "${OLD_JOB_PID}" "${JOB_TIMEOUT_SECONDS}" "${RUN_DIR}/logs/old-job.log"
    else
        wait_for_process "${OLD_JOB_PID}" "${JOB_TIMEOUT_SECONDS}" "${RUN_DIR}/logs/old-job.log"
    fi
    OLD_JOB_PID=""
    stop_process "${OLD_SERVER_PID}"
    OLD_SERVER_PID=""
    end_stage

    stage "Restore savepoint with current dev"
    CURRENT_SERVER_PID="$(start_cluster "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-server.log")"
    if is_endless_job; then
        CURRENT_RESTORE_PID="$(start_current_restore_job "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-restore.log")"
        sleep "${SAVEPOINT_DELAY_SECONDS}"
    else
        if ! restore_current_job "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-restore.log"; then
            tail -n 200 "${RUN_DIR}/logs/current-restore.log" >&2 || true
            fail "Restore failed on current dev"
        fi
    fi
    if ! assert_output "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-assert.log"; then
        if ! assert_output_with_retry "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-assert.log" "${JOB_TIMEOUT_SECONDS}"; then
            tail -n 200 "${RUN_DIR}/logs/current-assert.log" >&2 || true
            fail "Restored output assertion failed"
        fi
    fi
    if is_endless_job; then
        cancel_job "${CURRENT_DIST_DIR}" "${RUN_DIR}/logs/current-cancel.log" || true
        wait_for_process_to_stop "${CURRENT_RESTORE_PID}" "${JOB_TIMEOUT_SECONDS}" "${RUN_DIR}/logs/current-restore.log"
        CURRENT_RESTORE_PID=""
    fi
    stop_process "${CURRENT_SERVER_PID}"
    CURRENT_SERVER_PID=""
    run_scenario_hook "teardown.sh"
    SCENARIO_SETUP_STARTED="false"
    end_stage
}

main() {
    download_old_distribution

    stage "Extract distributions"
    local current_archive
    current_archive="$(find_current_distribution)"
    extract_distribution "${OLD_ARCHIVE}" "${OLD_DIST_DIR}"
    extract_distribution "${current_archive}" "${CURRENT_DIST_DIR}"
    end_stage

    install_old_release_connectors
    run_scenario

    echo "Upgrade compatibility scenario passed: ${OLD_SEATUNNEL_VERSION} -> current dev (${SCENARIO})"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
