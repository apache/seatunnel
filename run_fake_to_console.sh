#!/usr/bin/env bash
set -euo pipefail

SEATUNNEL_HOME="/Users/coolcorgy/data/whale/whaletunnel/seatunnel-dist/target/apache-seatunnel-2.6-WS-test-SNAPSHOT"
CONF_FILE="/Users/coolcorgy/data/whale/whaletunnel/seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/fake_to_console.conf"
JOB_NAME="我是 割裂"

mkdir -p "${SEATUNNEL_HOME}/logs"

"${SEATUNNEL_HOME}/bin/seatunnel.sh" \
  -m cluster \
  -c "${CONF_FILE}" \
  -n "${JOB_NAME}"
