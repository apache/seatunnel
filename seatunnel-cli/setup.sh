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

# SeaTunnel CLI - Quick Setup Script

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=== SeaTunnel CLI Setup ==="
echo ""

# Check Python version (>= 3.10 required)
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null) || {
    echo "Error: Python 3.10+ required but python3 not found"; exit 1;
}
PYTHON_MAJOR=${PYTHON_VERSION%%.*}
PYTHON_MINOR=${PYTHON_VERSION#*.}
if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 10 ]; }; then
    echo "Error: Python >= 3.10 required, but found Python $PYTHON_VERSION"
    exit 1
fi
echo "Python $PYTHON_VERSION"

VENV_DIR="$PWD/.venv"
venv_usable() {
    [ -x "$VENV_DIR/bin/python" ] &&
        "$VENV_DIR/bin/python" -c 'import sys; assert sys.prefix != sys.base_prefix and sys.version_info >= (3, 10)' >/dev/null 2>&1 &&
        "$VENV_DIR/bin/python" -m pip --version >/dev/null 2>&1
}

if venv_usable; then
    echo "Reusing $VENV_DIR"
else
    # Preserve a pre-existing but unusable environment for inspection.
    VENV_BACKUP=""
    if [ -e "$VENV_DIR" ] || [ -L "$VENV_DIR" ]; then
        VENV_BACKUP="${VENV_DIR}.invalid.$(date +%s).$$"
        mv "$VENV_DIR" "$VENV_BACKUP"
        echo "Preserved unusable environment at $VENV_BACKUP"
    fi

    if ! python3 -m venv "$VENV_DIR" || ! venv_usable; then
        # A failed venv attempt can leave files that virtualenv will not overwrite.
        rm -rf -- "$VENV_DIR"
        if ! command -v virtualenv >/dev/null 2>&1 ||
            ! virtualenv --python=python3 "$VENV_DIR" || ! venv_usable; then
            rm -rf -- "$VENV_DIR"
            if [ -n "$VENV_BACKUP" ]; then
                mv "$VENV_BACKUP" "$VENV_DIR"
            fi
            echo "Error: Python virtual environments require python3-venv (ensurepip) or virtualenv." >&2
            exit 1
        fi
    fi

fi

echo "Installing seatunnel CLI in $VENV_DIR..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install -e ".[dev]"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo ""
echo "  Activate this checkout's environment, or call .venv/bin/seatunnel directly:"
echo "     source .venv/bin/activate"
echo ""
echo "  1. Set SeaTunnel engine path (for /check, /run, connector metadata):"
echo ""
echo "     export SEATUNNEL_HOME=/path/to/apache-seatunnel"
echo ""
echo "     If not set, the CLI auto-detects from its package location."
echo "     In the distribution tarball this resolves to the tarball root, e.g.:"
echo "       apache-seatunnel-3.0.0/          <-- SEATUNNEL_HOME"
echo "       ├── bin/seatunnel.sh"
echo "       ├── cli/seatunnel_cli/            <-- CLI package"
echo "       ├── connectors/"
echo "       └── lib/"
echo ""
echo "  2. Configure your LLM provider — run the interactive setup wizard:"
echo ""
echo "     seatunnel --init"
echo ""
echo "     Or set environment variables manually (choose one):"
echo ""
echo "       # Anthropic API"
echo "       export AI_PROVIDER=anthropic"
echo "       export ANTHROPIC_API_KEY=sk-ant-..."
echo ""
echo "       # OpenAI / compatible API"
echo "       export AI_PROVIDER=openai"
echo "       export OPENAI_API_KEY=sk-..."
echo ""
echo "       # AWS Bedrock"
echo "       export AI_PROVIDER=bedrock"
echo "       export AWS_REGION=us-east-1"
echo ""
echo "       # OrcaRouter AI gateway (OpenAI-compatible)"
echo "       export AI_PROVIDER=orcarouter"
echo "       export ORCAROUTER_API_KEY=orc_..."
echo ""
echo "  3. Run the CLI:"
echo ""
echo "     seatunnel                                           # Interactive mode"
echo '     seatunnel "Sync MySQL users table to S3 Parquet"    # Single-shot mode'
echo ""
