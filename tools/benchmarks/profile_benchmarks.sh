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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
default_repository_dir=$(cd "${script_dir}/../.." && pwd)

usage() {
    cat <<'EOF'
Usage:
  profile_benchmarks.sh profile <cpu|wall|lock|gc> [options] [-- JMH_ARGS...]
  profile_benchmarks.sh capture jfr [options] [-- JMH_ARGS...]

Options:
  --benchmark REGEX  JMH selector. Defaults to $BENCHMARKS or one queue benchmark method.
                     The selector must resolve to exactly one benchmark method.
  --repository DIR   Repository containing the benchmark JAR. Defaults to this script's repository.
  --output DIR       Output directory. Defaults below seatunnel-benchmarks/target/profiles.
  -h, --help         Show this help.

Examples:
  profile_benchmarks.sh profile cpu
  profile_benchmarks.sh profile lock --benchmark 'IntermediateQueueBenchmark.blockingQueueRecordHandoff$'
  profile_benchmarks.sh profile gc -- -f 1 -wi 1 -i 1 -w 1s -r 1s
  profile_benchmarks.sh capture jfr --benchmark 'IntermediateQueueBenchmark.disruptorRecordHandoff$'
EOF
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
    usage
    exit 0
fi

if [[ $# -lt 2 ]]; then
    usage >&2
    exit 1
fi

command_name="$1"
mode="$2"
shift 2

case "${command_name}" in
    profile)
        case "${mode}" in
            cpu | wall | lock | gc) ;;
            *) fail "Unsupported profile mode '${mode}'. Expected cpu, wall, lock, or gc." ;;
        esac
        ;;
    capture)
        case "${mode}" in
            jfr) ;;
            *) fail "Unsupported capture mode '${mode}'. Expected jfr." ;;
        esac
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *) fail "Unsupported command '${command_name}'. Expected profile or capture." ;;
esac

benchmark_selector="${BENCHMARKS:-IntermediateQueueBenchmark.disruptorRecordHandoff$}"
repository_dir="${default_repository_dir}"
output_directory=""
jmh_arguments=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --benchmark)
            [[ $# -ge 2 ]] || fail "--benchmark requires a value"
            benchmark_selector="$2"
            shift 2
            ;;
        --output)
            [[ $# -ge 2 ]] || fail "--output requires a value"
            output_directory="$2"
            shift 2
            ;;
        --repository)
            [[ $# -ge 2 ]] || fail "--repository requires a value"
            repository_dir="$2"
            shift 2
            ;;
        --)
            shift
            jmh_arguments=("$@")
            break
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) fail "Unknown script option '$1'. Put JMH arguments after --." ;;
    esac
done

if [[ ${#jmh_arguments[@]} -eq 0 && -n "${PROFILE_JMH_ARGS:-}" ]]; then
    read -r -a jmh_arguments <<< "${PROFILE_JMH_ARGS}"
fi

if [[ "${repository_dir}" != /* ]]; then
    repository_dir="${PWD}/${repository_dir}"
fi
[[ -d "${repository_dir}" ]] || fail "Repository directory does not exist: ${repository_dir}"
repository_dir=$(cd "${repository_dir}" && pwd)
benchmark_jar="${repository_dir}/seatunnel-benchmarks/target/benchmarks.jar"
[[ -f "${benchmark_jar}" ]] || fail "Missing ${benchmark_jar}. Build it with: ./mvnw -Pbenchmark -pl seatunnel-benchmarks -am -DskipTests package"

fork_option_seen=false
for ((argument_index = 0; argument_index < ${#jmh_arguments[@]}; argument_index++)); do
    case "${jmh_arguments[argument_index]}" in
        -f)
            [[ "${fork_option_seen}" == "false" ]] || fail "Specify the JMH -f option only once."
            [[ $((argument_index + 1)) -lt ${#jmh_arguments[@]} ]] || fail "JMH -f requires a value."
            [[ "${jmh_arguments[argument_index + 1]}" == "1" ]] || fail "Diagnostic runs require exactly one fork; use '-f 1'."
            fork_option_seen=true
            ;;
        -f=*)
            [[ "${fork_option_seen}" == "false" ]] || fail "Specify the JMH -f option only once."
            [[ "${jmh_arguments[argument_index]}" == "-f=1" ]] || fail "Diagnostic runs require exactly one fork; use '-f 1'."
            fork_option_seen=true
            ;;
    esac
done
if [[ "${fork_option_seen}" == "false" ]]; then
    jmh_arguments+=( -f 1 )
fi

set +e
benchmark_listing=$(java -jar "${benchmark_jar}" "${benchmark_selector}" -l)
listing_status=$?
set -e
[[ ${listing_status} -eq 0 ]] || fail "Could not resolve the benchmark selector '${benchmark_selector}'."

resolved_benchmarks=()
while IFS= read -r benchmark_name; do
    case "${benchmark_name}" in
        "" | Benchmarks:*) continue ;;
        *) resolved_benchmarks+=( "${benchmark_name}" ) ;;
    esac
done <<< "${benchmark_listing}"

if [[ ${#resolved_benchmarks[@]} -eq 0 ]]; then
    fail "Benchmark selector '${benchmark_selector}' did not match a benchmark."
fi
if [[ ${#resolved_benchmarks[@]} -ne 1 ]]; then
    printf 'Matched benchmarks:\n' >&2
    printf '  %s\n' "${resolved_benchmarks[@]}" >&2
    fail "Benchmark diagnostics require one exact benchmark method; '${benchmark_selector}' matched ${#resolved_benchmarks[@]}."
fi

if [[ -z "${output_directory}" ]]; then
    run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
    output_directory="${repository_dir}/seatunnel-benchmarks/target/profiles/${command_name}-${mode}-${run_id}"
elif [[ "${output_directory}" != /* ]]; then
    output_directory="${PWD}/${output_directory}"
fi

if [[ -d "${output_directory}" ]]; then
    first_existing_artifact=$(find "${output_directory}" -mindepth 1 -print -quit)
    [[ -z "${first_existing_artifact}" ]] || fail "Output directory is not empty: ${output_directory}"
elif [[ -e "${output_directory}" ]]; then
    fail "Output path is not a directory: ${output_directory}"
fi

raw_directory="${output_directory}/raw"
pipeline_directory="${output_directory}/pipeline-results"
jmh_result="${output_directory}/result.jmh.json"
jmh_log="${output_directory}/jmh.log"
profile_json="${output_directory}/profile-report.json"
profile_markdown="${output_directory}/profile-summary.md"
mkdir -p "${raw_directory}" "${pipeline_directory}"

resolve_async_profiler_library() {
    local candidate
    for candidate in \
        "${ASYNC_PROFILER_HOME}/lib/libasyncProfiler.so" \
        "${ASYNC_PROFILER_HOME}/lib/libasyncProfiler.dylib"; do
        if [[ -f "${candidate}" ]]; then
            echo "${candidate}"
            return
        fi
    done
    fail "async-profiler library was not found under ASYNC_PROFILER_HOME."
}

convert_async_profiler_jfr() {
    local profile_jfr
    local profile_summary
    profile_jfr=$(find "${raw_directory}" -type f -name 'jfr-*.jfr' -print -quit)
    profile_summary=$(find "${raw_directory}" -type f -name 'summary-*.txt' -print -quit)
    [[ -n "${profile_jfr}" && -n "${profile_summary}" ]] || return 1

    local sample_count
    sample_count=$(
        awk -F ':' '/^Total samples/ {gsub(/[[:space:]]/, "", $2); print $2; exit}' \
            "${profile_summary}"
    )
    if [[ ! "${sample_count}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Could not read async-profiler sample count from ${profile_summary}." >&2
        return 1
    fi
    if [[ ${sample_count} -eq 0 ]]; then
        echo "async-profiler collected no ${mode} samples; no flame graph was generated."
        return 0
    fi

    local event_option="--${mode}"
    local profile_prefix
    profile_prefix=$(basename "${profile_jfr}" .jfr)
    profile_prefix=${profile_prefix#jfr-}
    local profile_directory
    profile_directory=$(dirname "${profile_jfr}")
    local forward_html="${profile_directory}/flame-${profile_prefix}-forward.html"
    local reverse_html="${profile_directory}/flame-${profile_prefix}-reverse.html"

    "${async_profiler_converter}" --output html "${event_option}" --threads \
        "${profile_jfr}" "${forward_html}"
    "${async_profiler_converter}" --output html "${event_option}" --threads --reverse \
        "${profile_jfr}" "${reverse_html}"
}

profiler_arguments=()
async_profiler_converter=""
case "${command_name}:${mode}" in
    profile:cpu | profile:wall | profile:lock)
        [[ -n "${ASYNC_PROFILER_HOME:-}" ]] || fail "Set ASYNC_PROFILER_HOME to the complete async-profiler installation."
        async_profiler_library=$(resolve_async_profiler_library)
        async_profiler_converter="${ASYNC_PROFILER_HOME}/bin/jfrconv"
        [[ -x "${async_profiler_converter}" ]] || fail "Missing async-profiler converter: ${async_profiler_converter}"
        event="${mode}"
        interval=10000000
        if [[ "${mode}" == "cpu" ]]; then
            event="${ASYNC_PROFILER_CPU_EVENT:-cpu}"
        fi
        if [[ "${mode}" == "lock" ]]; then
            # Lock profiling uses the interval as a contention-duration threshold. A low threshold
            # keeps short queue monitor contention visible; profiler scores remain diagnostic only.
            interval=10000
        fi
        profiler_arguments+=(
            -prof
            "async:libPath=${async_profiler_library};event=${event};interval=${interval};threads=true;output=jfr,text;dir=${raw_directory}"
        )
        ;;
    profile:gc)
        profiler_arguments+=(
            -prof
            "gc:alloc=true;churn=true;churnWait=500"
        )
        ;;
    capture:jfr)
        profiler_arguments+=(
            -prof
            "jfr:dir=${raw_directory};configName=profile;stackDepth=256"
        )
        ;;
esac

java_command=(
    java
    "-Dseatunnel.benchmark.result.dir=${pipeline_directory}"
    -jar
    "${benchmark_jar}"
    "${benchmark_selector}"
    -foe
    true
    -rf
    json
    -rff
    "${jmh_result}"
)
java_command+=("${profiler_arguments[@]}")
java_command+=("${jmh_arguments[@]}")

echo "Diagnostic: ${command_name} ${mode}"
echo "Benchmark: ${resolved_benchmarks[0]}"
echo "Output: ${output_directory}"
echo "JMH log: ${jmh_log}"

set +e
(
    cd "${repository_dir}"
    "${java_command[@]}"
) > "${jmh_log}" 2>&1
benchmark_status=$?
set -e

diagnostic_status=${benchmark_status}
if [[ ${benchmark_status} -eq 0 && -n "${async_profiler_converter}" ]]; then
    if ! convert_async_profiler_jfr; then
        diagnostic_status=1
    fi
fi

report_status=0
python3 "${script_dir}/profile_report.py" mode \
    --mode "${mode}" \
    --command "${command_name}" \
    --jmh "${jmh_result}" \
    --artifact-dir "${output_directory}" \
    --output-json "${profile_json}" \
    --output-md "${profile_markdown}" \
    --status "${diagnostic_status}" || report_status=$?

if [[ ${benchmark_status} -ne 0 ]]; then
    echo "JMH failed with status ${benchmark_status}; the last 200 log lines follow:" >&2
    tail -n 200 "${jmh_log}" >&2
    exit "${benchmark_status}"
fi
if [[ ${diagnostic_status} -ne 0 ]]; then
    echo "async-profiler post-processing failed; inspect ${profile_markdown}." >&2
    exit "${diagnostic_status}"
fi
echo "Diagnostic report: ${profile_markdown}"
exit "${report_status}"
