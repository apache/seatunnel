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

# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# One-command benchmark: API credentials in → test conclusion out.
#
#   ./benchmark/run_benchmark.sh --provider openai --model gpt-4o --api-key sk-...
#   ./benchmark/run_benchmark.sh --provider anthropic --model claude-sonnet-4-20250514 --api-key sk-ant-...
#   ./benchmark/run_benchmark.sh --provider openai --model deepseek-chat \
#       --api-key sk-... --base-url https://api.deepseek.com/v1
#   ./benchmark/run_benchmark.sh --models benchmark/models.json          # multi-model
#
# Extra args (--tiers, --tasks, --max-repairs, --level, --out) are passed through.
#
# Preflight (informational, never blocks):
#   - SEATUNNEL_HOME set + seatunnel.sh present?  → enables L2 dry-run
#   - Docker data env running? (offers to start it) → enables L3 execution
# The runner itself degrades gracefully, so this script always produces a
# report with whatever gates are available.

set -euo pipefail
cd "$(dirname "$0")/.."   # seatunnel-cli/

echo "── SeaTunnel AI CLI Benchmark ──────────────────────────"

# ── Preflight: python deps ──
if ! python3 -c "import pyhocon" 2>/dev/null; then
    echo "Installing seatunnel-cli (editable) with all providers..."
    pip install -q -e ".[all]"
fi

# ── Preflight: L2 (engine dry-run) ──
if [[ -n "${SEATUNNEL_HOME:-}" && -f "${SEATUNNEL_HOME}/bin/seatunnel.sh" ]]; then
    echo "✔ L2 enabled  — SEATUNNEL_HOME=${SEATUNNEL_HOME}"
else
    echo "✘ L2 disabled — SEATUNNEL_HOME not set or bin/seatunnel.sh missing."
    echo "               Build one with: ./mvnw -pl seatunnel-dist -am -DskipTests package"
fi

# ── Preflight: L3 (docker data env) ──
if command -v docker >/dev/null 2>&1; then
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^st-bench-mysql$'; then
        echo "✔ L3 enabled  — benchmark data environment is running"
    else
        echo "✘ L3 data env not running."
        if [[ -t 0 ]]; then
            read -r -p "  Start it now? (docker compose up -d --wait) [y/N] " reply
            if [[ "${reply,,}" == "y" ]]; then
                docker compose -f benchmark/docker/docker-compose.yml up -d --wait
                echo "✔ L3 enabled  — data environment started"
            fi
        else
            echo "  (non-interactive: start it with: docker compose -f benchmark/docker/docker-compose.yml up -d --wait)"
        fi
    fi
else
    echo "✘ L3 disabled — docker not available"
fi

echo "─────────────────────────────────────────────────────────"

python3 -m benchmark.runner "$@"

OUT_DIR="benchmark/results"
prev_arg=""
for arg in "$@"; do
    if [[ "$prev_arg" == "--out" ]]; then OUT_DIR="$arg"; fi
    prev_arg="$arg"
done

echo ""
echo "── Conclusion ──────────────────────────────────────────"
echo "Report:   ${OUT_DIR}/summary.md"
echo "Details:  ${OUT_DIR}/summary.csv, ${OUT_DIR}/results.json"
echo "Configs:  ${OUT_DIR}/configs/"
