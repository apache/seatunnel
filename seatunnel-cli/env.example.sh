#!/usr/bin/env bash
# SeaTunnel CLI - Environment Configuration
# Copy this file and source it: cp env.example.sh env.sh && source env.sh

# ─── Provider Selection ───
# Choose one: bedrock | anthropic | openai
export AI_PROVIDER=bedrock

# ─── Option A: AWS Bedrock Configuration (AI_PROVIDER=bedrock) ───
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
# export ANTHROPIC_BEDROCK_BASE_URL=https://bedrock-runtime.us-east-1.amazonaws.com
export ANTHROPIC_MODEL='global.anthropic.claude-opus-4-6-v1'
export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'
# export AWS_ACCESS_KEY_ID=your_access_key
# export AWS_SECRET_ACCESS_KEY=your_secret_key
# export AWS_SESSION_TOKEN=your_session_token

# ─── Option B: Anthropic API Configuration (AI_PROVIDER=anthropic) ───
# export ANTHROPIC_API_KEY=sk-ant-...
# export ANTHROPIC_MODEL=claude-sonnet-4-20250514
# export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001

# ─── Option C: OpenAI API Configuration (AI_PROVIDER=openai) ───
# export OPENAI_API_KEY=sk-...
# export OPENAI_MODEL=gpt-4o
# export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini
# export OPENAI_BASE_URL=                   # Optional: for compatible APIs (Azure, local, etc.)

# ─── SeaTunnel Engine ───
# export SEATUNNEL_HOME=/path/to/seatunnel
# export SEATUNNEL_API_BASE=http://localhost:5801

# ─── Optional ───
export DISABLE_PROMPT_CACHING=false
