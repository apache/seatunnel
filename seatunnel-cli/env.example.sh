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

# SeaTunnel CLI - Environment Configuration
# Copy this file and source it: cp env.example.sh env.sh && source env.sh

# ─── Provider Selection ───
# Uncomment ONE of the following provider blocks.
# Or run 'seatunnel --init' for interactive setup.

# export AI_PROVIDER=anthropic    # Option A
# export AI_PROVIDER=openai       # Option B
# export AI_PROVIDER=bedrock      # Option C

# ─── Option A: Anthropic API (AI_PROVIDER=anthropic) ───
# export ANTHROPIC_API_KEY=sk-ant-...
# export ANTHROPIC_MODEL=claude-sonnet-4-20250514                # optional override
# export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001    # optional override

# ─── Option B: OpenAI / Compatible API (AI_PROVIDER=openai) ───
# export OPENAI_API_KEY=sk-...
# export OPENAI_MODEL=gpt-4o                        # optional override
# export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini        # optional override
# export OPENAI_BASE_URL=                            # optional: for Azure, DeepSeek, local models, etc.

# ─── Option C: AWS Bedrock (AI_PROVIDER=bedrock) ───
# export AWS_REGION=us-east-1
# export ANTHROPIC_MODEL='us.anthropic.claude-sonnet-4-20250514-v1:0'          # optional override
# export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'  # optional override
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...

# ─── SeaTunnel Engine (optional) ───
# export SEATUNNEL_HOME=/path/to/seatunnel
# export SEATUNNEL_API_BASE=http://localhost:5801
