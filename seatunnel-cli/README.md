# SeaTunnel CLI

Generate [Apache SeaTunnel](https://seatunnel.apache.org/) data pipeline configurations using natural language.

Describe your data synchronization task in English or Chinese, and the CLI generates a production-ready HOCON config file with validation, auto-fix, and one-click execution.

## Features

- **Natural Language to Config** -- Describe what you want in plain English or Chinese, get a valid SeaTunnel config
- **Multi-Provider LLM** -- AWS Bedrock, Anthropic API, OpenAI (and compatible APIs like Azure OpenAI)
- **Multi-Agent Pipeline** -- Planner -> Generator -> Validator -> Auto-fix, up to 3 correction rounds
- **100+ Connectors** -- Full coverage of SeaTunnel's connector ecosystem with runtime metadata reflection
- **Transform Metadata** -- Source, sink, and transform plugins use full option rules and value constraints during generation
- **Skill Framework** -- Three-layer generation: Skill SOP -> Golden Example -> Connector Metadata
- **Auto-Save** -- Generated configs automatically saved to `.data/last_job.conf` (co-located with CLI)
- **Auto-Fix** -- `/check` and `/run` failures trigger automatic LLM-powered diagnosis and config repair
- **Session & Memory** -- Multi-turn conversation with persistent session history and connection detail memory
- **Dry-Run Validation** -- Local syntax check + engine `--check` + REST API validation
- **Bilingual** -- English and Chinese natural language input

## Prerequisites

- **Python 3.10+** (3.11 or 3.12 recommended)
- **pip** (bundled with Python 3.10+, used for automatic dependency installation)
- **Operating System**: macOS, Linux, or WSL on Windows
- One of the following LLM providers:
  - **AWS Bedrock** -- requires AWS credentials and `boto3`
  - **Anthropic API** -- requires `ANTHROPIC_API_KEY` and `anthropic` package
  - **OpenAI API** -- requires `OPENAI_API_KEY` and `openai` package
- (Optional) Running Apache SeaTunnel engine for live metadata and job execution

> **Note:** When launched via `bin/seatunnel-ai.sh`, Python dependencies are installed automatically on first run. No manual `pip install` needed.

## Installation

### From SeaTunnel Distribution (recommended)

No manual installation needed. The shell wrapper handles everything:

```bash
# First run — auto-installs dependencies, then launches interactive setup
bin/seatunnel-ai.sh --init

# After init, launch directly
bin/seatunnel-ai.sh
```

> **Note:** `bin/seatunnel-ai.sh` automatically sets `SEATUNNEL_HOME` to the distribution root directory. No manual configuration needed.

### From Source (development)

```bash
cd seatunnel-cli

# Quick setup (installs all providers + dev tools)
bash setup.sh
```

Then configure your LLM provider:

```bash
seatunnel --init
```

#### Manual install (alternative to setup.sh)

```bash
pip install -e ".[bedrock]"    # AWS Bedrock
pip install -e ".[anthropic]"  # Anthropic API
pip install -e ".[openai]"     # OpenAI API
pip install -e ".[all]"        # All providers
pip install -e ".[dev]"        # Development (all providers + pytest, ruff)
```

After `pip install`, the `seatunnel` command is available globally.

## Configuration

Copy the example environment file and configure your provider:

```bash
cp env.example.sh env.sh
# Edit env.sh with your settings
source env.sh
```

### Provider Configuration

#### Option A: AWS Bedrock (Default)

```bash
export AI_PROVIDER=bedrock
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1

# Model overrides (optional)
export ANTHROPIC_MODEL='us.anthropic.claude-sonnet-4-20250514-v1:0'
export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'

# Credentials: use AWS CLI profile, env vars, or IAM role
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...
```

The Bedrock provider preserves Claude `reasoningContent` blocks in streamed
Converse responses when Bedrock returns them.

#### Option B: Anthropic API

```bash
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# Model overrides (optional)
export ANTHROPIC_MODEL=claude-sonnet-4-20250514
export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001
```

The Anthropic provider preserves Claude thinking blocks (`thinking`, `signature`, and
`redacted_thinking`) in assistant history when the API returns them.

#### Option C: OpenAI API

```bash
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...

# Model overrides (optional)
export OPENAI_MODEL=gpt-4o
export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini

# Custom base URL for compatible APIs (Azure OpenAI, local models, etc.)
# export OPENAI_BASE_URL=https://your-endpoint.openai.azure.com/

# Keep enabled for compatible reasoning models that require reasoning_content replay
# export OPENAI_ECHO_REASONING_CONTENT=true
```

### SEATUNNEL_HOME

`SEATUNNEL_HOME` is the path to your Apache SeaTunnel engine installation. The CLI uses it to:

- **Connector metadata** — load `connector_metadata.json` for accurate connector option rules (150+ connectors)
- **`/check`** — run `seatunnel.sh --check` for engine-level config validation
- **`/run`** — execute jobs via `seatunnel.sh -e local` or REST API

```bash
export SEATUNNEL_HOME=/path/to/apache-seatunnel
```

**Default behavior:** If `SEATUNNEL_HOME` is not set, the CLI auto-detects from its package location. In the distribution tarball the CLI lives at `cli/seatunnel_cli/`, so it resolves two levels up to the tarball root:

```
apache-seatunnel-3.0.0/          <-- auto-detected as SEATUNNEL_HOME
├── bin/seatunnel.sh
├── bin/seatunnel-ai.sh
├── cli/seatunnel_cli/            <-- CLI package location
│   └── connector_metadata.json
├── connectors/
└── lib/
```

For source installs (`setup.sh` / `pip install`), the directory structure does not match, so you need to set `SEATUNNEL_HOME` manually.

> Without `SEATUNNEL_HOME`, the CLI can still generate configs using LLM knowledge, but `/check`, `/run`, and runtime connector metadata will be unavailable.

### SeaTunnel Engine (Optional)

For live connector metadata and REST API job submission, start the SeaTunnel server:

```bash
export SEATUNNEL_API_BASE=http://localhost:5801  # Default
```

When the engine is running, the CLI operates in **cluster mode** with live connector metadata, engine-level validation, and direct job submission via REST API. When unavailable, it falls back to **offline mode** using the bundled connector metadata.

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AI_PROVIDER` | No | `bedrock` | LLM provider: `bedrock`, `anthropic`, or `openai` |
| `AWS_REGION` | Bedrock | `us-east-1` | AWS region for Bedrock |
| `ANTHROPIC_API_KEY` | Anthropic | -- | Anthropic API key |
| `OPENAI_API_KEY` | OpenAI | -- | OpenAI API key |
| `OPENAI_BASE_URL` | No | -- | Custom endpoint for OpenAI-compatible APIs |
| `OPENAI_ECHO_REASONING_CONTENT` | No | `true` | Preserve and replay `reasoning_content` for OpenAI-compatible reasoning models such as DeepSeek or GLM thinking mode |
| `ANTHROPIC_MODEL` | No | Provider default | Override primary model ID |
| `ANTHROPIC_SMALL_FAST_MODEL` | No | Provider default | Override fast model ID |
| `OPENAI_MODEL` | No | `gpt-4o` | Primary model for OpenAI provider |
| `OPENAI_SMALL_FAST_MODEL` | No | `gpt-4o-mini` | Fast model for OpenAI provider |
| `SEATUNNEL_HOME` | No | Auto-detect | SeaTunnel installation directory. Auto-detected in distribution tarball; set manually for source install |
| `SEATUNNEL_API_BASE` | No | `http://localhost:5801` | SeaTunnel REST API endpoint |
| `SEATUNNEL_CLI_DATA` | No | `<cli-package>/.data/` | Override CLI data directory (sessions, memory, config) |

## Usage

> **Tip:** Use `bin/seatunnel-ai.sh` from the SeaTunnel distribution, or `seatunnel` after pip install. Both are equivalent.

### Interactive Mode

```bash
seatunnel
```

Launches an interactive REPL with streaming output, command history, session persistence, and multi-turn conversation.

### Single-Shot Mode

```bash
# Generate and display config
seatunnel "Sync MySQL users table to S3 Parquet"

# Generate and save to file
seatunnel "从 Kafka 读取订单数据写入 ClickHouse" -o my_job.conf

# Override provider on the fly
seatunnel "Read CSV files and write to Elasticsearch" --provider openai --model gpt-4o
```

### CLI Arguments

```
seatunnel [request] [options]

Positional:
  request                  Natural language request (omit for interactive mode)

Options:
  -o, --output PATH        Save generated config to file
  --provider PROVIDER      LLM provider: bedrock | anthropic | openai
  --model MODEL            Override primary model ID
  --fast-model MODEL       Override fast model ID
  --sync-catalog PATH      Regenerate connector catalog from SeaTunnel source
  -V, --version            Show version
  -h, --help               Show help message
```

### Interactive Commands

| Command | Description |
|---------|-------------|
| `/save <path>` | Save config to custom path (auto-saved to `.data/last_job.conf` on generation) |
| `/check` | Dry-run validate last config; auto-diagnoses and fixes on failure |
| `/run` | Execute last config via REST API or `seatunnel.sh`; auto-diagnoses on failure |
| `/connectors` | List all available sources, sinks, and transforms; transform option rules and constraints are supported during generation |
| `/sessions` | List recent conversation sessions |
| `/resume [id]` | Resume a previous session |
| `/new` | Start a fresh session |
| `/memory` | Show remembered facts (connection details, preferences) |
| `/remember <text>` | Save a fact to memory (e.g., connection string, preference) |
| `/forget <id>` | Delete a memory entry |
| `/clear` | Clear conversation history and start a new session |
| `/help` | Show help panel |
| `/quit` | Exit |

## Examples

### MySQL to S3 (Batch)

```
🐬 SeaTunnel > Sync the users table from MySQL to S3 as Parquet files

  ⚙️  Generating SeaTunnel config...
  ✅ Validating config (round 1)...

  📋 Generated SeaTunnel Config
  Config saved to: .data/last_job.conf
```

### Kafka to ClickHouse (Streaming)

```
🐬 SeaTunnel > 从 Kafka 的 orders topic 实时同步到 ClickHouse 的 orders 表

  📋 Generated SeaTunnel Config
  Config saved to: .data/last_job.conf

🐬 SeaTunnel > /check
  [1] Local validation: PASS
  [2] Engine --check:   PASS
  Dry-run PASSED — Config is ready to execute.

🐬 SeaTunnel > /run
  Job submitted: 1234567890 (orders-sync)
  Status: RUNNING
  Status: FINISHED
  Job completed successfully.
```

### Multi-Turn Refinement with Memory

```
🐬 SeaTunnel > /remember MySQL host=10.0.1.100 port=3306 database=orders

  Saved mem_01 (type: connection)

🐬 SeaTunnel > Sync PostgreSQL orders to Doris

  📋 Generated SeaTunnel Config
  Config saved to: .data/last_job.conf

🐬 SeaTunnel > Add a filter to only include orders where amount > 100

  📋 Generated SeaTunnel Config (updated)
  Config saved to: .data/last_job.conf

🐬 SeaTunnel > /save production_job.conf
  Config saved to: production_job.conf
```

> **Security:** `/remember` rejects entries containing passwords, API keys, tokens, or other credential values. Only non-sensitive facts (hostnames, ports, database names, table names) are stored.

### Auto-Fix on Failure

```
🐬 SeaTunnel > /run
  Job FAILED: Missing required option 'fs.s3a.endpoint'

  Diagnosing and fixing config...

  🔧 Fixed Config
  (added missing S3 endpoint configuration)
  Config saved to: .data/last_job.conf

  Use /check to validate, then /run to retry.
```

## Architecture

### Multi-Agent Pipeline

```
User Input (natural language)
     |
     v
+-----------------+     +----------------------+
|  Planner Agent  |---->|  Connector Knowledge |
|  (analyze intent|<----|  Base (tools)         |
|   + lookup info)|     +----------------------+
+--------+--------+
         | plan / chat
         v
+-----------------+
|  Config Agent   |  Generate HOCON config
+--------+--------+
         | config
         v
+-----------------+     +----------------------+
| Validator Agent |---->|  Local Validation    |
| (syntax + logic)|     |  + Engine --check    |
+--------+--------+     +----------------------+
         |
    PASS? ---- Yes --> Output + auto-save
         |
         No (max 3 rounds)
         |
         v
+-----------------+
|   Fix Agent     |  Auto-correct errors
+-----------------+
         |
    /run or /check failure
         |
         v
+-----------------+
|  Repair Agent   |  Diagnose + patch config
+-----------------+
```

### Connector Knowledge Base

Two-tier resolution with intelligent fallback:

1. **Runtime API** -- Live metadata from running SeaTunnel engine (`/option-rules` endpoint). Always accurate, zero maintenance.
2. **Bundled Metadata** -- `connector_metadata.json` ships with the CLI package. Source, sink, and transform plugins include full option rules and value constraints exported from SeaTunnel engine via reflection. Zero LLM token cost.

Transform metadata is resolved through the same path as source and sink metadata, so prompts can include transform-specific required options and constraints such as non-blank SQL queries.

### Memory System

The CLI remembers facts across sessions to improve config accuracy:

- **Project context** -- Table names, database names, common patterns.
- **Preferences** -- Parallelism, format, language preferences.

Memory is stored locally at `.data/memory.json` (co-located with the CLI package). Use `/remember` to add facts, `/memory` to view, `/forget` to remove.

### Validation Pipeline

| Phase | Method | Description |
|-------|--------|-------------|
| **Phase 1** | Local validation | HOCON syntax, structure, required params, brace matching, security checks |
| **Phase 2** | Engine `--check` | Invokes `seatunnel.sh --check` for engine-level validation |
| **Phase 3** | REST API | Optional validation via running SeaTunnel cluster |
| **Auto-fix** | LLM-powered | Up to 3 rounds of automatic error correction during generation |
| **Auto-repair** | LLM-powered | Automatic diagnosis and config patching on `/check` or `/run` failure |

## Connector Metadata

The CLI ships with `connector_metadata.json`, exported from the SeaTunnel engine via runtime reflection. It includes source, sink, and transform plugin option rules, conditional options, and value constraints. No extra steps needed.

To re-export for a different SeaTunnel version (requires a running engine):

```bash
# Export metadata from running engine
bin/seatunnel-metadata-export.sh
```

This calls the engine's option-rule reflection API for all registered connectors and writes the result to `connector_metadata.json`.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
