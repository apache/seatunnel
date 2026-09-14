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

SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null; pwd)"
source "${SCRIPT_DIR}/run_upgrade_compatibility.sh"

TEST_ROOT="$(mktemp -d)"
trap 'cleanup; rm -rf "${TEST_ROOT}"' EXIT

ROOT_DIR="${TEST_ROOT}"
DIST_DIR="${ROOT_DIR}/seatunnel-dist/target"

reset_distributions() {
    rm -rf "${DIST_DIR}"
    mkdir -p "${DIST_DIR}"
    CURRENT_DIST_ARCHIVE=""
}

assert_equals() {
    local expected="$1"
    local actual="$2"
    [ "${actual}" = "${expected}" ] || {
        echo "Expected '${expected}', got '${actual}'" >&2
        exit 1
    }
}

assert_contains() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == *"${expected}"* ]] || {
        echo "Expected '${actual}' to contain '${expected}'" >&2
        exit 1
    }
}

test_selects_regular_distribution() {
    reset_distributions
    local regular_archive="${DIST_DIR}/apache-seatunnel-3.0.0-SNAPSHOT-bin.tar.gz"
    touch "${regular_archive}"
    touch "${DIST_DIR}/apache-seatunnel-edge-agent-3.0.0-SNAPSHOT-bin.tar.gz"

    assert_equals "${regular_archive}" "$(find_current_distribution)"
}

test_uses_explicit_distribution() {
    reset_distributions
    local explicit_archive="${TEST_ROOT}/current-seatunnel.tar.gz"
    touch "${explicit_archive}"
    CURRENT_DIST_ARCHIVE="${explicit_archive}"

    assert_equals "${explicit_archive}" "$(find_current_distribution)"
}

test_rejects_missing_regular_distribution() {
    reset_distributions
    touch "${DIST_DIR}/apache-seatunnel-edge-agent-3.0.0-SNAPSHOT-bin.tar.gz"

    local output
    if output="$(find_current_distribution 2>&1)"; then
        echo "Expected missing regular distribution to fail" >&2
        exit 1
    fi
    assert_contains "No current dev distribution found" "${output}"
}

test_rejects_multiple_regular_distributions() {
    reset_distributions
    touch "${DIST_DIR}/apache-seatunnel-3.0.0-SNAPSHOT-bin.tar.gz"
    touch "${DIST_DIR}/apache-seatunnel-3.1.0-SNAPSHOT-bin.tar.gz"

    local output
    if output="$(find_current_distribution 2>&1)"; then
        echo "Expected multiple regular distributions to fail" >&2
        exit 1
    fi
    assert_contains "Multiple current dev distributions found" "${output}"
}

test_waits_for_engine_log() {
    local dist_dir="${TEST_ROOT}/distribution"
    local launch_log="${TEST_ROOT}/cluster-launch.log"
    mkdir -p "${dist_dir}/bin" "${dist_dir}/config" "${dist_dir}/logs"
    touch "${dist_dir}/config/seatunnel.yaml"
    cat > "${dist_dir}/bin/seatunnel-cluster.sh" <<'EOF'
#!/usr/bin/env bash
echo "start master_and_worker node"
echo "received new worker register" > "$(dirname "$0")/../logs/seatunnel-engine-server.log"
sleep 5
EOF
    chmod +x "${dist_dir}/bin/seatunnel-cluster.sh"

    STARTUP_TIMEOUT_SECONDS=2
    local pid
    pid="$(start_cluster "${dist_dir}" "${launch_log}")"
    [[ "${pid}" =~ ^[0-9]+$ ]] || {
        echo "Expected cluster process id, got '${pid}'" >&2
        exit 1
    }

    local child_pid
    child_pid="$(pgrep -P "${pid}" | head -n 1)"
    [ -n "${child_pid}" ] || {
        echo "Expected cluster process ${pid} to have a child" >&2
        exit 1
    }
    stop_process "${pid}"
    local attempt
    for attempt in {1..20}; do
        if ! kill -0 "${child_pid}" >/dev/null 2>&1; then
            break
        fi
        sleep 0.1
    done
    if kill -0 "${child_pid}" >/dev/null 2>&1; then
        echo "Expected child process ${child_pid} to stop with the cluster" >&2
        exit 1
    fi
}

test_selects_regular_distribution
test_uses_explicit_distribution
test_rejects_missing_regular_distribution
test_rejects_multiple_regular_distributions
test_waits_for_engine_log

echo "Upgrade compatibility runner tests passed"
