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
echo "Configure your AWS Bedrock credentials:"
echo ""
echo "  export AWS_DEFAULT_REGION=us-east-1"
echo "  export AWS_REGION=us-east-1"
echo "  export ANTHROPIC_BEDROCK_BASE_URL=https://bedrock-runtime.us-east-1.amazonaws.com"
echo "  export ANTHROPIC_MODEL='us.anthropic.claude-sonnet-4-20250514-v1:0'"
echo "  export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'"
echo ""
echo "Run the CLI:"
echo ""
echo "  # Interactive mode"
echo "  seatunnel"
echo ""
echo "  # Single-shot mode"
echo '  seatunnel "Sync MySQL users table to S3 Parquet"'
echo ""
echo '  # With output file'
echo '  seatunnel "从 Kafka 读取订单数据写入 ClickHouse" -o my_job.conf'
echo ""
