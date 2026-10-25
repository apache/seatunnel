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

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly CHART_DIR="${SCRIPT_DIR}/../seatunnel"
readonly RELEASE_NAME="seatunnel-e2e"
readonly RELEASE_NAMESPACE="seatunnel-e2e"
readonly REST_RESPONSE="${TMPDIR:-/tmp}/seatunnel-helm-rest-response.json"

render_chart() {
    helm lint --strict "${CHART_DIR}"
    helm template "${RELEASE_NAME}" "${CHART_DIR}" \
        --namespace "${RELEASE_NAMESPACE}" \
        --set master.replicas=1 \
        --set worker.replicas=1 \
        > /dev/null
    helm template "${RELEASE_NAME}" "${CHART_DIR}" \
        --namespace "${RELEASE_NAMESPACE}" \
        --set ingress.enabled=true \
        --set ingress.tls.enabled=true \
        --set master.replicas=1 \
        --set worker.replicas=1 \
        > /dev/null
}

print_diagnostics() {
    kubectl --namespace "${RELEASE_NAMESPACE}" get all --output wide || true
    kubectl --namespace "${RELEASE_NAMESPACE}" describe pods || true
    kubectl --namespace "${RELEASE_NAMESPACE}" logs \
        --selector "app.kubernetes.io/instance=${RELEASE_NAME}" \
        --all-containers \
        --prefix \
        --tail=200 || true
}

cleanup() {
    local exit_code=$?
    trap - EXIT

    if [[ ${exit_code} -ne 0 ]]; then
        print_diagnostics
    fi
    helm uninstall "${RELEASE_NAME}" --namespace "${RELEASE_NAMESPACE}" >/dev/null 2>&1 || true
    kubectl delete namespace "${RELEASE_NAMESPACE}" --wait=false >/dev/null 2>&1 || true
    exit "${exit_code}"
}

verify_rest_api() {
    rm -f "${REST_RESPONSE}"

    for _ in $(seq 1 60); do
        if kubectl --namespace "${RELEASE_NAMESPACE}" exec \
            "deployment/${RELEASE_NAME}-master" -- \
            curl --fail --silent --show-error --max-time 5 \
            http://127.0.0.1:8080/system-monitoring-information \
            > "${REST_RESPONSE}" \
            && response_has_expected_members; then
            return
        fi
        sleep 2
    done

    echo "SeaTunnel REST API did not report both cluster members" >&2
    return 1
}

response_has_expected_members() {
    python3 - "${REST_RESPONSE}" <<'PY'
import json
import pathlib
import sys

response_path = pathlib.Path(sys.argv[1])
if not response_path.exists():
    raise SystemExit(1)

try:
    members = json.loads(response_path.read_text(encoding="utf-8"))
except (json.JSONDecodeError, OSError):
    raise SystemExit(1)

raise SystemExit(0 if isinstance(members, list) and len(members) >= 2 else 1)
PY
}

install_chart() {
    trap cleanup EXIT

    helm upgrade --install "${RELEASE_NAME}" "${CHART_DIR}" \
        --namespace "${RELEASE_NAMESPACE}" \
        --create-namespace \
        --wait \
        --timeout 15m \
        --set master.replicas=1 \
        --set worker.replicas=1

    kubectl --namespace "${RELEASE_NAMESPACE}" rollout status \
        "deployment/${RELEASE_NAME}-master" --timeout=2m
    kubectl --namespace "${RELEASE_NAMESPACE}" rollout status \
        "deployment/${RELEASE_NAME}-worker" --timeout=2m
    verify_rest_api
}

case "${1:-}" in
    render)
        render_chart
        ;;
    install)
        install_chart
        ;;
    *)
        echo "Usage: $0 <render|install>" >&2
        exit 2
        ;;
esac
