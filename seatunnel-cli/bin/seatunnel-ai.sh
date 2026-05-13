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

# SeaTunnel AI CLI — Generate SeaTunnel configs with natural language.
#
# Usage:
#   bin/seatunnel-ai.sh                                    # Interactive mode
#   bin/seatunnel-ai.sh "Sync MySQL users to S3 Parquet"   # Single-shot
#   bin/seatunnel-ai.sh --init                             # First-time setup
#   bin/seatunnel-ai.sh --help                             # Show all options
#
# First run: bin/seatunnel-ai.sh --init  (installs Python deps + configures LLM)

set -e

# ─── Resolve SEATUNNEL_HOME ───
PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

BIN_DIR=$(cd "$(dirname "$PRG")" >/dev/null; pwd)
APP_DIR=$(cd "$BIN_DIR/.." >/dev/null; pwd)

# Set SEATUNNEL_HOME if not already set
export SEATUNNEL_HOME="${SEATUNNEL_HOME:-$APP_DIR}"

CLI_DIR="$APP_DIR/cli"

# ─── Check Python ───
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 not found. Python 3.10+ is required."
    echo "Install: https://www.python.org/downloads/"
    exit 1
fi

PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info.minor)' 2>/dev/null)
if [ "$PYTHON_MINOR" -lt 10 ] 2>/dev/null; then
    echo "Error: Python 3.10+ required, found $(python3 --version)"
    exit 1
fi

# ─── Install on first run or --init ───
NEED_INSTALL=false

if [ "$1" = "--init" ]; then
    NEED_INSTALL=true
elif ! python3 -c "import seatunnel_cli" 2>/dev/null; then
    NEED_INSTALL=true
    echo "First run detected — installing seatunnel-cli..."
else
    # Guard against global/stale package: verify the installed package
    # actually comes from this distribution's cli/ directory.
    INSTALLED_PATH=$(python3 -c "import seatunnel_cli; print(seatunnel_cli.__file__)" 2>/dev/null || true)
    if [[ "$INSTALLED_PATH" != "$CLI_DIR"* ]]; then
        NEED_INSTALL=true
        echo "Installed seatunnel-cli is not from this distribution — reinstalling..."
    fi
fi

if [ "$NEED_INSTALL" = true ]; then
    if [ ! -f "$CLI_DIR/pyproject.toml" ]; then
        echo "Error: CLI package not found at $CLI_DIR"
        echo "Expected: $CLI_DIR/pyproject.toml"
        exit 1
    fi

    echo "Installing seatunnel-cli dependencies..."
    if ! python3 -m pip install "$CLI_DIR[all]" --quiet; then
        echo "ERROR: Failed to install seatunnel-cli. Please check your Python/pip setup." >&2
        echo "  Try manually: cd $CLI_DIR && pip install '.[all]'" >&2
        exit 1
    fi
    echo "  SEATUNNEL_HOME: $SEATUNNEL_HOME"
    echo ""

    if [ "$1" = "--init" ]; then
        python3 -m seatunnel_cli.cli --init
        exit 0
    fi
fi

# ─── Source env.sh if exists ───
if [ -f "$CLI_DIR/env.sh" ]; then
    source "$CLI_DIR/env.sh"
fi

# ─── Run CLI ───
exec python3 -m seatunnel_cli.cli "$@"
