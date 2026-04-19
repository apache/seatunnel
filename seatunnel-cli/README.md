# SeaTunnel CLI

Generate [Apache SeaTunnel](https://seatunnel.apache.org/) data pipeline configurations using natural language.

Describe your data synchronization task in English or Chinese, and the CLI generates a production-ready HOCON config file with validation, auto-fix, and one-click execution.

## Features

- **Natural Language to Config** -- Describe what you want in plain English or Chinese, get a valid SeaTunnel config
- **Multi-Provider LLM** -- AWS Bedrock, Anthropic API, OpenAI (and compatible APIs like Azure OpenAI)
- **Multi-Agent Pipeline** -- Planner -> Generator -> Validator -> Auto-fix, up to 3 correction rounds
- **100+ Connectors** -- Full coverage of SeaTunnel's connector ecosystem with auto-generated metadata catalog
- **Auto-Save** -- Generated configs automatically saved to `~/.seatunnel/last_job.conf`
- **Auto-Fix** -- `/check` and `/run` failures trigger automatic LLM-powered diagnosis and config repair
- **Session & Memory** -- Multi-turn conversation with persistent session history and connection detail memory
- **Dry-Run Validation** -- Local syntax check + engine `--check` + REST API validation
- **Bilingual** -- English and Chinese natural language input

## Requirements

- Python >= 3.10
- One of the following LLM providers:
  - **AWS Bedrock** -- requires AWS credentials and `boto3`
  - **Anthropic API** -- requires `ANTHROPIC_API_KEY` and `anthropic` package
  - **OpenAI API** -- requires `OPENAI_API_KEY` and `openai` package
- (Optional) Running Apache SeaTunnel engine for live metadata and job execution

## Installation

### Quick Setup

```bash
cd seatunnel-cli
bash setup.sh
```

### Manual Installation

```bash
cd seatunnel-cli

# Install with your preferred provider
pip install -e ".[bedrock]"    # AWS Bedrock
pip install -e ".[anthropic]"  # Anthropic API
pip install -e ".[openai]"     # OpenAI API
pip install -e ".[all]"        # All providers
pip install -e ".[dev]"        # Development (all providers + pytest, ruff)
```

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

#### Option B: Anthropic API

```bash
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# Model overrides (optional)
export ANTHROPIC_MODEL=claude-sonnet-4-20250514
export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001
```

#### Option C: OpenAI API

```bash
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...

# Model overrides (optional)
export OPENAI_MODEL=gpt-4o
export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini

# Custom base URL for compatible APIs (Azure OpenAI, local models, etc.)
# export OPENAI_BASE_URL=https://your-endpoint.openai.azure.com/
```

### SeaTunnel Engine (Optional)

For live connector metadata, dry-run validation, and job execution:

```bash
export SEATUNNEL_HOME=/path/to/apache-seatunnel
export SEATUNNEL_API_BASE=http://localhost:5801  # Default
```

When the engine is running, the CLI operates in **cluster mode** with live connector metadata, engine-level validation, and direct job submission via REST API. When unavailable, it falls back to **offline mode** using the built-in static connector catalog.

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AI_PROVIDER` | No | `bedrock` | LLM provider: `bedrock`, `anthropic`, or `openai` |
| `AWS_REGION` | Bedrock | `us-east-1` | AWS region for Bedrock |
| `ANTHROPIC_API_KEY` | Anthropic | -- | Anthropic API key |
| `OPENAI_API_KEY` | OpenAI | -- | OpenAI API key |
| `OPENAI_BASE_URL` | No | -- | Custom endpoint for OpenAI-compatible APIs |
| `ANTHROPIC_MODEL` | No | Provider default | Override primary model ID |
| `ANTHROPIC_SMALL_FAST_MODEL` | No | Provider default | Override fast model ID |
| `OPENAI_MODEL` | No | `gpt-4o` | Primary model for OpenAI provider |
| `OPENAI_SMALL_FAST_MODEL` | No | `gpt-4o-mini` | Fast model for OpenAI provider |
| `SEATUNNEL_HOME` | No | -- | SeaTunnel installation directory |
| `SEATUNNEL_API_BASE` | No | `http://localhost:5801` | SeaTunnel REST API endpoint |

## Usage

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
| `/save <path>` | Save config to custom path (auto-saved to `~/.seatunnel/last_job.conf` on generation) |
| `/check` | Dry-run validate last config; auto-diagnoses and fixes on failure |
| `/run` | Execute last config via REST API or `seatunnel.sh`; auto-diagnoses on failure |
| `/connectors` | List all available sources, sinks, and transforms |
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
  Config saved to: ~/.seatunnel/last_job.conf
```

### Kafka to ClickHouse (Streaming)

```
🐬 SeaTunnel > 从 Kafka 的 orders topic 实时同步到 ClickHouse 的 orders 表

  📋 Generated SeaTunnel Config
  Config saved to: ~/.seatunnel/last_job.conf

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
🐬 SeaTunnel > /remember MySQL host=10.0.1.100 port=3306 user=etl password=etl123

  Saved mem_01 (type: connection)

🐬 SeaTunnel > Sync PostgreSQL orders to Doris

  📋 Generated SeaTunnel Config
  Config saved to: ~/.seatunnel/last_job.conf

🐬 SeaTunnel > Add a filter to only include orders where amount > 100

  📋 Generated SeaTunnel Config (updated)
  Config saved to: ~/.seatunnel/last_job.conf

🐬 SeaTunnel > /save production_job.conf
  Config saved to: production_job.conf
```

### Auto-Fix on Failure

```
🐬 SeaTunnel > /run
  Job FAILED: The value of property fs.s3a.access-key must not be null

  Diagnosing and fixing config...

  🔧 Fixed Config
  (added missing S3 credentials)
  Config saved to: ~/.seatunnel/last_job.conf

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

Three-tier resolution with intelligent fallback:

1. **Runtime API** -- Live metadata from running SeaTunnel engine (`/option-rules` endpoint). Always accurate, zero maintenance.
2. **Auto-Generated Catalog** -- 100+ connectors with 1200+ options, extracted from Java source code via regex. Cached at `~/.seatunnel/catalog/`. Zero LLM token cost.
3. **Keyword Routing** -- Maps 50+ natural language terms (English and Chinese) to connector names. Examples: "mysql" -> `[Jdbc, MySQL-CDC]`, "kafka" -> `[Kafka]`.

### Memory System

The CLI remembers facts across sessions to improve config accuracy:

- **Connection details** -- Host, port, user, password, access keys. Used directly in generated configs instead of `${VAR}` placeholders.
- **Project context** -- Table names, database names, common patterns.
- **Preferences** -- Parallelism, format, language preferences.

Memory is stored locally at `~/.seatunnel/memory.json`. Use `/remember` to add facts, `/memory` to view, `/forget` to remove.

### Validation Pipeline

| Phase | Method | Description |
|-------|--------|-------------|
| **Phase 1** | Local validation | HOCON syntax, structure, required params, brace matching, security checks |
| **Phase 2** | Engine `--check` | Invokes `seatunnel.sh --check` for engine-level validation |
| **Phase 3** | REST API | Optional validation via running SeaTunnel cluster |
| **Auto-fix** | LLM-powered | Up to 3 rounds of automatic error correction during generation |
| **Auto-repair** | LLM-powered | Automatic diagnosis and config patching on `/check` or `/run` failure |

## Connector Catalog

The CLI ships with a built-in connector catalog (`connector_catalog.json`) covering 100+ connectors. No extra steps needed.

To regenerate for a different SeaTunnel version:

```bash
# From SeaTunnel source code
seatunnel --sync-catalog /path/to/seatunnel

# Or clone first
git clone https://github.com/apache/seatunnel.git /tmp/seatunnel
seatunnel --sync-catalog /tmp/seatunnel
```

This scans `*Factory.java` and `*Options.java`, extracts connector metadata via regex, resolves option inheritance chains, and updates the bundled catalog. First scan takes ~60s, subsequent loads from cache take ~6ms.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
