---
sidebar_position: 1
---

# AI CLI Overview

The SeaTunnel AI CLI generates production-ready SeaTunnel pipeline configurations from natural language. Describe a data synchronization task in English or Chinese, and the CLI produces a validated HOCON config file — with automatic validation, error repair, and one-click execution.

```
🐬 SeaTunnel > Sync the users table from MySQL to S3 as Parquet files

  ⚙️  Generating SeaTunnel config...
  ✅ Validating config (round 1)...

  📋 Generated SeaTunnel Config
  Config saved to: .data/last_job.conf
```

The AI CLI ships as the `seatunnel-cli` module inside the main SeaTunnel repository and is packaged into the standard distribution. It is launched via `bin/seatunnel-ai.sh` from a distribution, or installed from source with pip.

## Key Capabilities

- **Natural language to config** — English or Chinese input, complete HOCON output
- **Multi-provider LLM** — AWS Bedrock (including OpenAI-family models via the `bedrock-mantle` endpoint), Anthropic API, OpenAI and compatible APIs, OrcaRouter AI gateway
- **Multi-agent pipeline** — Planner → Config Generator → Validator → auto-fix, with up to 3 correction rounds
- **Connector knowledge** — 150+ connectors with full option rules and value constraints, resolved from a live engine or bundled metadata
- **Validation & repair** — local checks, engine `--check`/dry-run, and LLM-powered diagnosis-and-repair when `/check` or `/run` fails
- **Session & memory** — multi-turn refinement, persistent sessions, connection-detail memory (credentials are never stored)

## Documentation in this Section

| Page | Contents |
|------|----------|
| [Quick Start](quickstart.md) | Install, configure a provider, generate your first pipeline |
| [Design](design.md) | The multi-agent architecture and validation pipeline |
| [Model Benchmark](benchmark.md) | Measured accuracy across 7 LLMs, model selection guidance, and known weak scenarios |

## Relationship to Other AI Tooling

The AI CLI is the built-in, runtime-integrated tool. Complementary external tooling lives in the [SeaTunnel Tools](https://github.com/apache/seatunnel-tools) repository: the [SeaTunnel Skill](../tools/seatunnel-skill.md) (Claude integration for IDE-level assistance), the [MCP Server](../tools/seatunnel-mcp.md) (programmatic LLM access to SeaTunnel resources), and [x2seatunnel](../tools/x2seatunnel.md) (config conversion).
