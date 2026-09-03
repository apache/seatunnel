---
sidebar_position: 4
---

# Model Benchmark

The AI CLI's accuracy is measured — not assumed — by a dedicated benchmark: 100 tasks in three complexity tiers, judged through layered verdict gates up to **real job execution** against Dockerized data sources, across 7 mainstream LLMs. This page summarizes the methodology, the results, and what they mean for choosing a model.

> Results below were measured in July 2026 against seatunnel-cli v0.1.0 (commit `59ada4ec0`) with models served by AWS Bedrock. Accuracy drifts as models and the CLI evolve; treat the numbers as a snapshot and re-run the benchmark for current values.

## Methodology

**Task suite**: 100 tasks — 20 simple (single source→sink), 45 medium (type mapping, CDC, transform chains, multi-table), 35 complex (multi-source DAGs, fan-out, conditional routing) — across 12 ETL scenario categories, with 10 Chinese-language prompts and 18 rule probes targeting known LLM failure modes (conditional option misuse, BATCH/STREAMING inference, routing-label wiring).

**Verdict gates**, mirroring the CLI's own check → dry-run → run pipeline:

| Gate | Verdict | Catches |
|------|---------|---------|
| L1 static | HOCON parse + connector metadata + assertions | Syntax, wrong connector, missing options |
| L3 real execution | Job runs on the official `apache/seatunnel` image against real MySQL/PostgreSQL/Kafka/ClickHouse/Elasticsearch; batch = exit code, streaming = 60s health | Everything "looks right but doesn't run" |

**Repair loop**: on failure, the failing gate's real error output is fed to the CLI's own repair agent, up to 3 rounds. All verdicts are deterministic — no LLM-as-judge.

## Headline Result: Static Rankings Invert Under Real Execution

| Model | Static gate (L1) | Real execution (L1+L3) |
|-------|:---:|:---:|
| Claude Opus 4.8 | 89% (rank 3) | **85% (rank 1)** |
| GPT-5.6 Sol | 90% (rank 2) | 81% (rank 2) |
| GPT-5.6 Terra | **93% (rank 1)** | 74% (rank 3) |

The static leader wrote configs that *looked* correct but failed at runtime (−20pp static→real decay); the static #3 wrote configs that ran (−6pp decay). **"Looks correct" and "actually runs" are different model capabilities** — which is why this benchmark executes configs for real, and why accuracy claims based on static checks alone should be treated with caution.

## Full Static-Gate Ranking (7 models)

| Model | Pass (≤5 repairs) | First-try pass | Repaired |
|-------|:---:|:---:|:---:|
| GPT-5.6 Terra | 93% | 79% | +14 |
| GPT-5.6 Sol | 90% | 80% | +10 |
| Claude Opus 4.8 | 89% | **87%** | +2 |
| Claude Sonnet 5 | 82% | 78% | +4 |
| Claude Fable 5 | 80% | 73% | +7 |
| Qwen3-Coder-Next | 67% | 53% | +14 |
| DeepSeek V3.2 | 58% | 58% | +0 |

## Model Behavior Profiles

Generation ability and repair ability are **independent dimensions** — and family traits are consistent:

- **First-try-correct (Anthropic family)**: Opus 4.8 has the highest first-try rate (87% static / 77% real) and the smallest static→real decay — what it writes tends to run. But its repair contribution is minimal (+2): it rarely recovers from its own failures.
- **Iterative-repair (GPT-5.6 Terra)**: unremarkable first-try, but the strongest repairer — it fixed 48% of its real runtime failures, climbing from 55% to 74%. Its drafts carry the most "looks-right-but-won't-run" risk.
- **Repair-disabled (DeepSeek V3.2)**: zero successful repairs in 100 tasks — its repair attempts return the original config or give up. With a product built around an auto-repair loop, this effectively halves the product's mechanism.
- Notable: the deep-reasoning flagship (Fable 5) ranked only 5th — it over-asks clarifying questions where the task calls for sensible defaults. And on the 10 Chinese-language tasks the Chinese-developed models scored *lower* (Qwen 5/10, DeepSeek 4/10) than GPT-5.6 Sol (10/10).

## Model Selection Guide

| Scenario | Recommendation | Why |
|----------|----------------|-----|
| Interactive use (user waiting) | GPT-5.6 Sol / Terra | Fast (25–58s/task), high combined accuracy |
| Unattended / batch generation | Claude Opus 4.8 | Highest first-try rate; output most likely to run unmodified |
| Budget-sensitive regression runs | Qwen3-Coder-Next | Lowest cost tier; acceptable for smoke testing, not production generation |
| Avoid for auto-repair workflows | DeepSeek V3.2 | Repair success rate of zero in measurement |

## Known Weak Scenarios (all models)

Thirteen tasks failed for **every** top-3 model. If your pipeline matches one of these shapes, review the generated config before production use:

| Scenario | Failure root cause |
|----------|--------------------|
| Doris / StarRocks sinks | Connector option knowledge gaps (fenodes, load ports, save modes) |
| PostgreSQL-CDC | Missing prerequisites (replication slots, publications) not reflected in config |
| Conditional routing (one source split to multiple sinks by predicate) | `plugin_output`/`plugin_input` wiring with parallel SQL transforms |
| Wide DAGs (5+ blocks, mixed transforms) | Label pairing and block-ordering mistakes |

These clusters are engineering targets, not permanent limits: they are being addressed through connector-metadata injection and golden-example coverage, with improvements verified against this same benchmark.

## Repair Loop: Measured Effectiveness

Feeding **real engine errors** back to the repair agent recovered 47% of runtime failures (top-3 models combined). The same model repairs structured validation errors at ~2× the rate of raw Java stack traces — evidence that structured error parsing, not a smarter model, is the highest-leverage next improvement for the repair loop.

## Running the Benchmark Yourself

The benchmark harness ships in the main repository under
[`seatunnel-cli/benchmark/`](https://github.com/apache/seatunnel/tree/dev/seatunnel-cli/benchmark) —
100 declarative tasks, the layered verdict gates, the Docker data
environment, and the report generator.

```bash
cd seatunnel-cli

# Credentials via provider-standard environment variables
export OPENAI_API_KEY=sk-...          # or ANTHROPIC_API_KEY / AWS credentials

# One command: installs deps, preflights the environment, runs, reports
./benchmark/run_benchmark.sh --provider openai --model gpt-4o

# Multi-model comparison
./benchmark/run_benchmark.sh --models benchmark/models.json

# Optional deeper gates:
#   L2 (engine dry-run)   — set SEATUNNEL_HOME to a dev-branch build
#   L3 (real execution)   — docker compose -f benchmark/docker/docker-compose.yml up -d --wait
```

Missing infrastructure degrades gracefully: without an engine or Docker you
get a static-gate (L1) report; trials whose requested gates could not execute
are excluded from every pass metric and flagged in the summary, so results
from differently-equipped machines are never silently compared.

Every report is stamped with the CLI version and git commit under test —
rerun the same model on a new CLI build to measure the impact of any prompt,
metadata, or repair-logic change on identical tasks. See
[`benchmark/README.md`](https://github.com/apache/seatunnel/blob/dev/seatunnel-cli/benchmark/README.md)
for the full methodology, task-suite layout, and metric definitions.
