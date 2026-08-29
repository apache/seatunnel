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
# export AI_PROVIDER=bedrock-mantle  # Option C2: OpenAI-family models on Bedrock
#                                    #   (GPT-5.6 Terra/Sol; needs ".[bedrock-mantle]" extra;
#                                    #    model via OPENAI_MODEL, e.g. openai.gpt-5.6-terra)
# export AI_PROVIDER=orcarouter   # Option D: OrcaRouter AI gateway (needs ".[openai]" extra)

# ─── Option A: Anthropic API (AI_PROVIDER=anthropic) ───
# export ANTHROPIC_API_KEY=sk-ant-...
# export ANTHROPIC_MODEL=claude-sonnet-4-20250514                # optional override
# export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001    # optional override

# ─── Option B: OpenAI / Compatible API (AI_PROVIDER=openai) ───
# export OPENAI_API_KEY=sk-...
# export OPENAI_MODEL=gpt-4o                        # optional override
# export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini        # optional override
# export OPENAI_BASE_URL=                            # optional: for Azure, DeepSeek, local models, etc.
# export OPENAI_ECHO_REASONING_CONTENT=true          # optional: keep true for reasoning models that require replay

# ─── Option C: AWS Bedrock (AI_PROVIDER=bedrock) ───
# export AWS_REGION=us-east-1
# export ANTHROPIC_MODEL='us.anthropic.claude-sonnet-4-20250514-v1:0'          # optional override
# export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'  # optional override
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...

# ─── Option D: OrcaRouter AI gateway (AI_PROVIDER=orcarouter) ───
# OpenAI-compatible gateway: many models behind one endpoint, model IDs use a
# provider/model namespace (e.g. deepseek/deepseek-v4-pro, openai/gpt-5.5-pro).
# The special model `orcarouter/auto` auto-grades and auto-routes each request.
# Requires: pip install -e ".[openai]"
# export ORCAROUTER_API_KEY=orc_...
# export ORCAROUTER_MODEL=orcarouter/auto                  # optional override
# export ORCAROUTER_SMALL_FAST_MODEL=orcarouter/auto       # optional override
# export ORCAROUTER_ECHO_REASONING_CONTENT=true            # optional: keep true to replay reasoning_content for reasoning models

# ─── SeaTunnel Engine (optional) ───
# export SEATUNNEL_HOME=/path/to/seatunnel
# export SEATUNNEL_API_BASE=http://localhost:5801
