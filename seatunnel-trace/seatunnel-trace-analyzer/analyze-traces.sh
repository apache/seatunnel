#!/bin/bash
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

# SeaTunnel StainTrace Analyzer - Shell Wrapper
#
# Usage:
#   ./analyze-traces.sh [input_dir] [output_html] [job_id] [date] [--bottleneck]
#
# Examples:
#   ./analyze-traces.sh /tmp/seatunnel/traces report.html
#   ./analyze-traces.sh /tmp/seatunnel/traces report.html 123456
#   ./analyze-traces.sh /tmp/seatunnel/traces report.html 123456 2024-01-01 --bottleneck
#
# Deployment:
#   Copy this script alongside the JAR file (or into a lib/ subdirectory):
#     trace-analyzer/
#     ├── analyze-traces.sh
#     └── seatunnel-trace-analyzer-*-jar-with-dependencies.jar   ← same dir
#   OR:
#     trace-analyzer/
#     ├── analyze-traces.sh
#     └── lib/
#         └── seatunnel-trace-analyzer-*-jar-with-dependencies.jar

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Default values
INPUT_DIR="${1:-/tmp/seatunnel/traces}"
OUTPUT_HTML="${2:-trace-report.html}"
JOB_ID="$3"
DATE="$4"

# --bottleneck flag: accepted as $5 or anywhere in the argument list
ENABLE_BOTTLENECK=false
for arg in "$@"; do
    if [ "$arg" = "--bottleneck" ] || [ "$arg" = "-b" ]; then
        ENABLE_BOTTLENECK=true
    fi
done

# ---------------------------------------------------------------------------
# Locate the JAR.
# Search order:
#   1. $SCRIPT_DIR                (production: JAR placed alongside the script)
#   2. $SCRIPT_DIR/lib            (production: JAR placed in a lib/ subdirectory)
#   3. $SCRIPT_DIR/target         (development: Maven build output)
# ---------------------------------------------------------------------------
JAR_PATH=""
for search_dir in "$SCRIPT_DIR" "$SCRIPT_DIR/lib" "$SCRIPT_DIR/target"; do
    if [ -d "$search_dir" ]; then
        found=$(ls "$search_dir"/seatunnel-trace-analyzer-*-jar-with-dependencies.jar 2>/dev/null | head -1)
        if [ -n "$found" ]; then
            JAR_PATH="$found"
            break
        fi
    fi
done

if [ -z "$JAR_PATH" ]; then
    echo "Error: seatunnel-trace-analyzer-*-jar-with-dependencies.jar not found."
    echo ""
    echo "Searched in:"
    echo "  $SCRIPT_DIR"
    echo "  $SCRIPT_DIR/lib"
    echo "  $SCRIPT_DIR/target"
    echo ""
    echo "To build from source, run from the project root:"
    echo "  mvn clean package -pl seatunnel-trace/seatunnel-trace-analyzer -am"
    echo ""
    echo "Then place the JAR alongside this script or in a lib/ subdirectory."
    exit 1
fi

echo "SeaTunnel StainTrace Analyzer"
echo "=============================="
echo "JAR: $JAR_PATH"
echo "Input directory: $INPUT_DIR"
echo "Output file: $OUTPUT_HTML"
[ -n "$JOB_ID" ] && echo "Job ID filter: $JOB_ID"
[ -n "$DATE" ] && echo "Date filter: $DATE"
$ENABLE_BOTTLENECK && echo "Bottleneck analysis: enabled"
echo ""

# Build command as an array so paths with spaces are handled correctly
CMD_ARGS=("java" "-jar" "$JAR_PATH" "-i" "$INPUT_DIR" "-o" "$OUTPUT_HTML")

[ -n "$JOB_ID" ]     && CMD_ARGS+=("-j" "$JOB_ID")
[ -n "$DATE" ]       && CMD_ARGS+=("-d" "$DATE")
$ENABLE_BOTTLENECK   && CMD_ARGS+=("-b")

"${CMD_ARGS[@]}"
