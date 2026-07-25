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

# Option A2: OpenAI-family Bedrock models (Responses-API-only, e.g. gpt-5.6-terra)
export AI_PROVIDER=bedrock-mantle
export OPENAI_MODEL='openai.gpt-5.6-terra'

# Option B: Anthropic API
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# Option C: OpenAI or compatible
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...
# export OPENAI_BASE_URL=https://...   # Azure OpenAI, DeepSeek, local vLLM, ...
```

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
