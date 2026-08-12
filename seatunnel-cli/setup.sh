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

set -e

echo "=== SeaTunnel CLI Setup ==="
echo ""

# Check Python version (>= 3.10 required)
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null) || {
    echo "Error: Python 3.10+ required but python3 not found"; exit 1;
}
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)
if [ "$PYTHON_MINOR" -lt 10 ]; then
    echo "Error: Python >= 3.10 required, but found Python $PYTHON_VERSION"
    exit 1
fi
echo "Python $PYTHON_VERSION"

# Install in development mode
echo "Installing seatunnel CLI..."
python3 -m pip install -e ".[dev]"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
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
echo "  3. Run the CLI:"
echo ""
echo "     seatunnel                                           # Interactive mode"
echo '     seatunnel "Sync MySQL users table to S3 Parquet"    # Single-shot mode'
echo ""
