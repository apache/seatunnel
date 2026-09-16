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

: "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is required}"

# Driver scripts and suites must come from the revision that defines the workflow, not from the
# baseline under test. This keeps comparisons compatible with older refs whose benchmark tooling
# does not yet understand the current workflow inputs or report format.
benchmark_tools_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

print_selection=false
if [[ "${1:-}" == "--print-selection" ]]; then
    print_selection=true
    shift
fi
if [[ "$#" -ne 0 ]]; then
    echo "Usage: $0 [--print-selection]" >&2
    exit 1
fi

benchmark_selector="${BENCHMARKS:-benchmarks_core}"
benchmark_custom_selector="${CUSTOM_BENCHMARKS:-}"
benchmark_suite_name="${BENCHMARK_SUITE:-}"
if [[ -n "${benchmark_custom_selector}" ]]; then
    benchmark_selector="${benchmark_custom_selector}"
    benchmark_suite_name=""
fi
if [[ -z "${benchmark_suite_name}" && "${benchmark_selector}" == "benchmarks_core" ]]; then
    benchmark_suite_name="benchmarks_core"
fi
if [[ -n "${benchmark_custom_selector}" ]]; then
    benchmark_selection_source="custom selector"
elif [[ -n "${benchmark_suite_name}" ]]; then
    benchmark_selection_source="suite"
else
    benchmark_selection_source="selector"
fi
benchmark_regex="${benchmark_selector}"
benchmark_pr_number="${PR_NUMBER:-}"
benchmark_ref="${SEATUNNEL_REF:-dev}"
benchmark_run_id="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
benchmark_runner_name="${RUNNER_NAME:-local}"
benchmark_runner_os="${RUNNER_OS:-unknown}"
benchmark_runner_arch="${RUNNER_ARCH:-unknown}"
benchmark_runner_image="${ImageOS:-unknown}-${ImageVersion:-unknown}"

if [[ -n "${benchmark_suite_name}" ]]; then
    if [[ ! "${benchmark_suite_name}" =~ ^[a-zA-Z0-9_-]+$ ]]; then
        echo "Invalid benchmark suite name: ${benchmark_suite_name}" >&2
        exit 1
    fi
    benchmark_suite_file="${benchmark_tools_dir}/suites/${benchmark_suite_name}.txt"
    if [[ ! -f "${benchmark_suite_file}" ]]; then
        echo "Benchmark suite does not exist: ${benchmark_suite_file}" >&2
        exit 1
    fi
    benchmark_regex=$(awk '
        /^[[:space:]]*($|#)/ { next }
        found { printf "|" }
        { printf "%s", $0; found = 1 }
        END { if (found) printf "\n" }
    ' "${benchmark_suite_file}")
    if [[ -z "${benchmark_regex}" ]]; then
        echo "Benchmark suite is empty: ${benchmark_suite_file}" >&2
        exit 1
    fi
fi

if [[ -z "${benchmark_suite_name}" && "${benchmark_regex}" == ".*" ]]; then
    if [[ -n "${benchmark_pr_number}" ]]; then
        echo \
            "WARNING: The explicit full-suite PR comparison can exceed the 240-minute workflow limit; prefer benchmarks_core, a benchmark class, or a custom selector." \
            >&2
    else
        echo \
            "WARNING: The explicit full-suite run can exceed the 240-minute workflow limit; prefer benchmarks_core, a benchmark class, or a custom selector." \
            >&2
    fi
fi

if [[ "${print_selection}" == "true" ]]; then
    printf 'benchmark_selection_source=%s\n' "${benchmark_selection_source}"
    printf 'benchmark_suite=%s\n' "${benchmark_suite_name}"
    printf 'benchmark_selector=%s\n' "${benchmark_selector}"
    printf 'benchmark_regex=%s\n' "${benchmark_regex}"
    exit 0
fi

: "${JAVA_VERSION:?JAVA_VERSION is required}"

benchmark_artifact_dir="${GITHUB_WORKSPACE}/benchmark-artifacts/java${JAVA_VERSION}"
mkdir -p "${benchmark_artifact_dir}"

echo "SeaTunnel ref: ${benchmark_ref}"
echo "Baseline commit: $(git -C baseline rev-parse HEAD)"
echo "Benchmark selection source: ${benchmark_selection_source}"
echo "Benchmark suite: ${benchmark_suite_name:-custom}"
echo "Benchmark selector: ${benchmark_selector}"
echo "Benchmark regex: ${benchmark_regex}"
if [[ -n "${benchmark_pr_number}" ]]; then
    echo "PR number: ${benchmark_pr_number}"
    echo "Candidate commit: $(git -C candidate rev-parse HEAD)"
fi

{
    echo "benchmark_regex=${benchmark_regex}"
    echo "runner_name=${benchmark_runner_name}"
    echo "runner_os=${benchmark_runner_os}"
    echo "runner_arch=${benchmark_runner_arch}"
    echo "runner_image=${benchmark_runner_image}"
    uname -a
    lscpu
    nproc
    free -h
    java -version 2>&1
} > "${benchmark_artifact_dir}/environment.txt"

benchmark_cpu_model=$(awk -F: '/model name/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}' /proc/cpuinfo)
benchmark_memory_kib=$(awk '/MemTotal/ {print $2; exit}' /proc/meminfo)

run_benchmark() {
    local checkout_dir="$1"
    local label="$2"
    local source_ref="$3"
    local forks="$4"
    local result_dir="${benchmark_artifact_dir}/${label}"
    local pipeline_dir="${result_dir}/pipeline-results"
    local result_prefix="${result_dir}/seatunnel-benchmarks"
    local commit
    commit=$(git -C "${checkout_dir}" rev-parse HEAD)
    mkdir -p "${pipeline_dir}"
    echo "${label}: ${forks} JMH fork(s)"

    (
        cd "${checkout_dir}"
        java -Dseatunnel.benchmark.result.dir="${pipeline_dir}" \
            -jar seatunnel-benchmarks/target/benchmarks.jar "${benchmark_regex}" \
            -f "${forks}" \
            -wi 3 \
            -i 5 \
            -foe true \
            -rf json \
            -rff "${result_prefix}.jmh.json"
    )

    python3 "${benchmark_tools_dir}/save_jmh_result.py" \
        --jmh "${result_prefix}.jmh.json" \
        --pipeline-dir "${pipeline_dir}" \
        --output-json "${result_prefix}.report.json" \
        --ref "${source_ref}" \
        --commit "${commit}" \
        --java "${JAVA_VERSION}" \
        --environment "github-hosted-ubuntu-24.04" \
        --runner-os "${benchmark_runner_os}" \
        --runner-arch "${benchmark_runner_arch}" \
        --runner-name "${benchmark_runner_name}" \
        --runner-image "${benchmark_runner_image}" \
        --kernel "$(uname -srvmo)" \
        --cpu-model "${benchmark_cpu_model}" \
        --cpu-count "$(nproc)" \
        --memory-kib "${benchmark_memory_kib}" \
        --run-id "${benchmark_run_id}-${label}" \
        --suite "${benchmark_suite_name:-${benchmark_selector}}"
}

if [[ -z "${benchmark_pr_number}" ]]; then
    run_benchmark baseline result "${benchmark_ref}" 3
    python3 "${benchmark_tools_dir}/regression_report.py" \
        --input "${benchmark_artifact_dir}/result/seatunnel-benchmarks.report.json" \
        --output "${benchmark_artifact_dir}/summary.md"
else
    # The ABBA sequence already provides two independent forked JVM runs per revision. Keeping one
    # fork in each outer run avoids making the default comparison exceed the job limit.
    run_benchmark baseline baseline-1 "${benchmark_ref}" 1
    run_benchmark candidate candidate-1 "PR #${benchmark_pr_number}" 1
    run_benchmark candidate candidate-2 "PR #${benchmark_pr_number}" 1
    run_benchmark baseline baseline-2 "${benchmark_ref}" 1
    python3 "${benchmark_tools_dir}/regression_report.py" \
        --baseline "${benchmark_artifact_dir}/baseline-1/seatunnel-benchmarks.report.json" \
        --baseline "${benchmark_artifact_dir}/baseline-2/seatunnel-benchmarks.report.json" \
        --input "${benchmark_artifact_dir}/candidate-1/seatunnel-benchmarks.report.json" \
        --input "${benchmark_artifact_dir}/candidate-2/seatunnel-benchmarks.report.json" \
        --output "${benchmark_artifact_dir}/summary.md"
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "${benchmark_artifact_dir}/summary.md" >> "${GITHUB_STEP_SUMMARY}"
else
    cat "${benchmark_artifact_dir}/summary.md"
fi
