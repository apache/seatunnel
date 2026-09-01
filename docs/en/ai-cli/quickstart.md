---
sidebar_position: 2
---

# Quick Start

## Prerequisites

- Python 3.10+ (3.11 or 3.12 recommended)
- macOS, Linux, or WSL on Windows
- One LLM provider credential:
  - **AWS Bedrock** — AWS credentials (profile, env vars, or IAM role)
  - **Anthropic API** — `ANTHROPIC_API_KEY`
  - **OpenAI API** (or compatible) — `OPENAI_API_KEY`
  - **OrcaRouter** — `ORCAROUTER_API_KEY`
- (Optional) a SeaTunnel installation for engine-level validation and job execution

## Install

### From the SeaTunnel distribution (recommended)

The shell wrapper installs Python dependencies automatically on first run:

```bash
# First run — installs dependencies, launches interactive setup
bin/seatunnel-ai.sh --init

# After init, launch directly
bin/seatunnel-ai.sh
```

`SEATUNNEL_HOME` is set automatically to the distribution root.

### From source

```bash
cd seatunnel-cli
bash setup.sh          # installs all providers + dev tools
seatunnel --init       # interactive provider setup
```

## Configure a Provider

```bash
# Option A: AWS Bedrock (default)
export AI_PROVIDER=bedrock
export AWS_REGION=us-east-1

# Option A2: OpenAI-family Bedrock models (bedrock-mantle) — see the
# dedicated section below for the full contract
export AI_PROVIDER=bedrock-mantle
export OPENAI_MODEL='openai.gpt-5.6-terra'

# Option B: Anthropic API
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# Option C: OpenAI or compatible
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...
# export OPENAI_BASE_URL=https://...   # Azure OpenAI, DeepSeek, local vLLM, ...

# Option D: OrcaRouter AI gateway
export AI_PROVIDER=orcarouter
export ORCAROUTER_API_KEY=orc_...
# Model IDs use a provider/model namespace (e.g. deepseek/deepseek-v4-pro);
# `orcarouter/auto` auto-grades and auto-routes each request.
# export ORCAROUTER_MODEL=orcarouter/auto
# export ORCAROUTER_SMALL_FAST_MODEL=orcarouter/auto
# export ORCAROUTER_ECHO_REASONING_CONTENT=true   # optional: replay reasoning_content for reasoning models
```

### OrcaRouter AI gateway

[OrcaRouter](https://www.orcarouter.ai) is an OpenAI-compatible AI gateway
that exposes many models — Claude, GPT, Gemini, DeepSeek, Qwen and more —
behind a single endpoint (`https://api.orcarouter.ai/v1`). Model IDs follow a
`provider/model` namespace, and the special `orcarouter/auto` model
automatically selects the best model per request. Configure it as a
first-class provider:

```bash
# Requires the openai package (shares the ".[openai]" extra)
pip install -e ".[openai]"

export AI_PROVIDER=orcarouter
export ORCAROUTER_API_KEY=orc_...
# export ORCAROUTER_MODEL=deepseek/deepseek-v4-pro    # optional override
# export ORCAROUTER_SMALL_FAST_MODEL=orcarouter/auto  # optional override
# export ORCAROUTER_ECHO_REASONING_CONTENT=true       # optional: replay reasoning_content

seatunnel "Sync MySQL users table to S3 Parquet"
```

The provider speaks the OpenAI Chat Completions protocol, so it fully supports
the CLI's internal tool-calling loop (connector lookups during planning),
streaming output, multi-turn sessions, and reasoning-content replay for
compatible reasoning models.

### bedrock-mantle: OpenAI-family models on Bedrock

Some OpenAI models on Bedrock (e.g. `openai.gpt-5.6-terra`, `openai.gpt-5.6-sol`)
are not in the Bedrock foundation-model catalog and only support the OpenAI
**Responses API** on the dedicated `bedrock-mantle` endpoint — the regular
`bedrock` provider (Converse API) and the `openai` provider (Chat Completions)
cannot reach them. Use the `bedrock-mantle` provider:

```bash
# 1. Install the provider extra (openai SDK >= 2.45 + AWS token generator)
pip install -e ".[bedrock-mantle]"

# 2. Configure — AWS credentials only, no OpenAI account or API key needed
export AI_PROVIDER=bedrock-mantle
export AWS_REGION=us-east-1                    # us-east-1 / us-east-2 / us-west-2
export OPENAI_MODEL='openai.gpt-5.6-terra'     # default if unset
# export OPENAI_SMALL_FAST_MODEL='openai.gpt-5.6-terra'

# 3. Generate as usual
seatunnel "Sync MySQL users table to S3 Parquet"
```

Provider contract:

- **Endpoint**: `https://bedrock-mantle.{region}.api.aws/openai/v1` — the
  model-specific `openai/v1` path required by these models (the generic `v1`
  Responses path rejects them).
- **Auth**: a short-term bearer token is derived automatically from your AWS
  credentials (profile, env vars, or IAM role) via `aws-bedrock-token-generator`
  and refreshed every 30 minutes. No long-lived key is stored anywhere.
- **Data retention**: every request is sent with `store=false`, so Bedrock does
  not retain your prompts or generated configs server-side (the service default
  would otherwise keep them for 30 days).
- **Parameters**: these models reject `temperature`; the provider never sends
  it, so any configured temperature value is not applied.
- **Errors**: truncated (`incomplete`), failed, and refused responses raise an
  explicit error instead of being returned as a normal answer.

The provider fully supports the CLI's internal tool-calling loop (connector
lookups during planning) and multi-turn sessions, including replay of the
model's reasoning output between tool calls.

API keys are read from environment variables only — they are never written to config files.

## Generate Your First Pipeline

### Single-shot mode

```bash
seatunnel "Sync MySQL users table to S3 Parquet"
seatunnel "从 Kafka 读取订单数据写入 ClickHouse" -o my_job.conf
```

### Interactive mode

```bash
seatunnel
```

```
🐬 SeaTunnel > Sync PostgreSQL orders to Doris

  📋 Generated SeaTunnel Config
  Config saved to: .data/last_job.conf

🐬 SeaTunnel > Add a filter to only include orders where amount > 100

  📋 Generated SeaTunnel Config (updated)

🐬 SeaTunnel > /check
  [1] Local validation: PASS
  [2] Engine --check:   PASS
  Dry-run PASSED — Config is ready to execute.

🐬 SeaTunnel > /run
  Job submitted: 1234567890 (orders-sync)
  Status: FINISHED
```

### Useful commands

| Command | Description |
|---------|-------------|
| `/check` | Validate the last config; auto-diagnoses and fixes on failure |
| `/run` | Execute via REST API or `seatunnel.sh`; auto-repairs on failure |
| `/connectors` | List available sources, sinks, and transforms |
| `/remember <text>` | Save a non-sensitive fact (hosts, ports, database names) |
| `/sessions`, `/resume` | List and resume previous conversations |

## Tips for Good Results

- **Include connection details in the prompt** (host, port, database, table): the config comes back runnable instead of full of placeholders.
- **State batch vs real-time intent explicitly** ("one-off full copy" vs "capture changes continuously") — this drives BATCH/STREAMING and CDC-connector selection.
- **Credentials are placeholdered by design**: generated configs reference `${MYSQL_PASSWORD}`-style variables; export them before `/run`.
- For scenarios the models are measurably weak at (conditional routing, PostgreSQL-CDC prerequisites, Doris/StarRocks options — see the [benchmark](benchmark.md)), review the generated config before running it in production.
