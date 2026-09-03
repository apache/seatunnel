# SeaTunnel AI CLI Accuracy Benchmark

A standalone benchmark (developer tooling — not part of the shipped CLI package,
not wired into `seatunnel-e2e`) that measures how accurately the `seatunnel-cli`
multi-agent pipeline turns natural-language requests into **runnable** SeaTunnel
configs, compared **across LLM providers/models**, **across ETL scenario types**,
through **three strict verification gates**, with an **auto-repair curve**
(pass@1 / pass@≤3 / pass@≤5).

## The three gates

| Gate | Command | Proves | Requires |
|------|---------|--------|----------|
| **L1 static** | in-process (`scoring.py`) | HOCON parses, expected connectors/mode used, required options present, task assertions hit | nothing |
| **L2 dry-run** | `seatunnel.sh --dry-run static` (PR #10763) | the engine itself accepts the config: plugin loadability, OptionRule required/unknown keys, option types, DAG topology, SQL transform syntax — same factory resolution as the real runtime parser | `SEATUNNEL_HOME` → a built SeaTunnel dist |
| **L3 execute** | `seatunnel.sh --config … -m local` | the job actually runs against real data sources/sinks. Batch: exit code 0. Streaming: healthy for 60 s (then killed). Optional per-task sink row-count probes | `SEATUNNEL_HOME` + the Docker data env below |

Each gate is a strict superset of certainty. A task **passes** only when every
enabled gate passes. Gates degrade gracefully: without `SEATUNNEL_HOME` the run
is L1-only; without Docker it is L1+L2.

## Auto-repair curve

After a failed gate, the failing layer's error output is fed to the CLI's own
repair agent (the same one behind `/check`/`/run` auto-fix) and the config is
re-evaluated — up to `--max-repairs` rounds (default 5). Each task records
`first_pass_round`:

- `pass@1` — first-try success (round 0), the RFC's "First-Run Pass Rate".
  **Measurement unit is the CLI product, not the bare model**: the CLI's
  generation pipeline internally validates and may fix a config up to 3
  times before delivering it, and round 0 means "the CLI's first delivered
  config". The number of internal fix rounds is recorded per trial as
  `internal_repair_rounds` (in results.json and summary.csv), so both
  timelines are observable. Benchmark-level repairs (driven by gate
  errors) are counted separately in `pass@≤k`.
- **Gate coverage**: a requested layer that cannot execute (missing engine,
  Docker service down, task-level skip) is recorded in `skipped_layers`
  with `all_gates_executed=false` for that trial. Such a trial is
  **excluded from every advertised success metric** — `first_pass_round`
  is only set when *all requested gates executed and passed*
  (`full_gate_passed`), so pass@1 / pass@≤k / pass^k / per-tier and
  per-category rates and the CSV `passed` column can never be inflated by
  partial coverage. The summary prints a per-model warning with the count
  of incomplete-coverage trials.
- **L3 verdict scope**: batch L3 verifies process exit (plus sink row
  counts for tasks with a `verify` block); streaming L3 verifies 60s
  healthy execution. This catches "config doesn't run" errors; it does not
  prove full semantic equivalence of the output data for every task —
  extending per-task `verify` probes is the intended path for that.
- `pass@≤3` — success within 3 repair rounds
- `pass@≤5` — success within 5 repair rounds

With `--trials k` (recommended: 2–3 for serious evaluation) each task runs k
independent times and the report adds **pass^k** — the fraction of tasks where
*every* trial succeeded (τ-bench-style reliability, what "production ready"
actually requires).

Every report is stamped with the **seatunnel-cli version + git commit** under
test, so runs are comparable across CLI improvements — rerun the same model on
a new CLI build to measure the improvement, holding the model constant.

## Task suite

**100 tasks** across three complexity tiers (RFC Section 5.2), each tagged with
an ETL scenario `category` for per-scenario reporting:

| Tier | File | Count | Focus |
|------|------|-------|-------|
| 1 — Simple | `tasks/tier1_simple.json` | 20 | saturation check: single source→sink, file ETL, basic streaming |
| 2 — Medium | `tasks/tier2_medium.json` | 45 | discrimination: type mapping (8), CDC (8), transform chains (8), multi-table (5), streaming (6), batch semantics |
| 3 — Complex | `tasks/tier3_complex.json` | 35 | multi-source DAG (11), fan-out (6), multi-pipeline (7), conditional routing (4) |

18 tasks are **failure probes** (`category: failure_probe`) that deliberately
target known LLM error patterns — conditional-option misuse (text-only options
on Parquet output), BATCH/STREAMING mode inference, missing checkpoint.interval,
routing-label pairing, source/sink option mixing, SQL-transform capability
limits. These directly measure rule knowledge rather than general fluency.

10 prompts are Chinese. All connection details in prompts match the Docker
environment, so generated configs are executable as-is. SQL-transform tasks
stay within the Zeta SQL engine's actual capability (projection + WHERE;
no GROUP BY / JOIN / ORDER BY — verified against `ZetaSQLEngine`).
99 of 100 tasks run through L3 (the one MinIO S3File task stops at L2).

Task format:

```json
{
  "id": "t2_mysql_filter_localfile",
  "category": "transform_chain",
  "prompt": "Read the transactions table from MySQL ... amount > 100 ...",
  "expect": {
    "source": ["Jdbc"], "sink": ["LocalFile"], "job_mode": "BATCH",
    "transform": ["Sql"],
    "must_match": ["(?i)where", "(?i)amount\\s*>\\s*100"]
  },
  "execution": {
    "services": ["mysql"],          // required docker services
    "mode": "batch",                // batch | streaming (L3 verdict rule)
    "l3": "run",                    // run | skip
    "setup_files": { "...": "..." },// optional local input files
    "verify": {                     // optional sink probe after success
      "container": "st-bench-clickhouse",
      "command": ["clickhouse-client", "..."],
      "expect_min_rows": 1
    }
  }
}
```

## Docker data environment

`docker/docker-compose.yml` provides sources and sinks with pre-seeded data
(image versions follow the official `seatunnel-e2e` suite):

| Service | Image | Endpoint | Credentials | Seeded content |
|---------|-------|----------|-------------|----------------|
| MySQL (binlog/GTID on, CDC-ready) | `mysql:8.0.43` | `localhost:3306` | root / Test@123 | db `shop`: users, orders, payments, products, audit_log, transactions, fact_sales + sink tables orders_rt, users_replica |
| PostgreSQL (`wal_level=logical`) | `postgres:14-alpine` | `localhost:15432` (host) / `postgres:5432` (in-network); task prompts use the canonical `5432`, the harness rewrites per backend | bench / Test@123 | db `analytics`: app_logs, customers, web_events, inventory |
| Kafka (KRaft) | `apache/kafka:3.7.0` | `localhost:9092` | PLAINTEXT | topics clicks, order_events, dbz.shop.users (seeded), events, wms.inventory, shop.orders.changelog |
| ClickHouse | `clickhouse/clickhouse-server:23.3.13.6` | `localhost:8123` | default / Test@123 | db `bench`: pre-created sink tables |
| Elasticsearch | `elasticsearch:8.9.0` | `localhost:9200` | security disabled | — |
| MinIO (S3) | `minio/minio` | `localhost:9000` | minioadmin / minioadmin | bucket `bench` |
| Doris *(profile `olap`)* | `apache/doris:doris-all-in-one-2.1.0` | FE 8030 / query 9030 | root / empty | apply `init/doris/01_bench.sql` |
| StarRocks *(profile `olap`)* | `starrocks/allin1-ubuntu:3.3.4` | FE 8031 / query 9031 | root / empty | apply `init/doris/01_bench.sql` |

```bash
cd seatunnel-cli/benchmark/docker
docker compose up -d --wait                 # core services
docker compose --profile olap up -d         # + Doris & StarRocks (heavy)
```

Generated configs use `${MYSQL_PASSWORD}`-style placeholders; the runner
resolves them by passing `-i KEY=VALUE` variables (`execution.CREDENTIALS`)
to `seatunnel.sh`, so no secrets ever land in config files or the repo.

## Quick start — API key in, conclusion out

```bash
cd seatunnel-cli

# Put credentials in provider-standard environment variables (recommended —
# secrets passed as CLI arguments leak into shell history and process lists):
export OPENAI_API_KEY=sk-...          # or ANTHROPIC_API_KEY / AWS credentials

# single model, one command (auto-installs deps, preflights L2/L3, prints report)
./benchmark/run_benchmark.sh --provider openai --model gpt-4o
./benchmark/run_benchmark.sh --provider anthropic --model claude-sonnet-4-20250514
OPENAI_BASE_URL=https://api.deepseek.com/v1 \
    ./benchmark/run_benchmark.sh --provider openai --model deepseek-chat   # any OpenAI-compatible API
./benchmark/run_benchmark.sh --provider bedrock --model us.anthropic.claude-sonnet-4-20250514-v1:0
                                                                    # bedrock uses AWS credentials

# multi-model comparison
./benchmark/run_benchmark.sh --models benchmark/models.json
```

`--api-key` is still accepted for throwaway keys in disposable environments,
but prefer the environment variables above for anything real.

The script never blocks on missing infrastructure: without `SEATUNNEL_HOME`
you get an L1 report, with it L1+L2, with the Docker env L1+L2+L3 — the
summary always states which gates ran and on how many tasks.

## Running (manual)

```bash
cd seatunnel-cli
pip install -e ".[all]"

# 1. (optional, enables L2+L3) build & point at a SeaTunnel dist
#    ./mvnw -pl seatunnel-dist -am -DskipTests -P release package
export SEATUNNEL_HOME=/path/to/apache-seatunnel-<version>

# 2. (optional, enables L3) start the data environment
docker compose -f benchmark/docker/docker-compose.yml up -d --wait

# 3. configure providers + model matrix
export ANTHROPIC_API_KEY=... ; export OPENAI_API_KEY=...
cp benchmark/models.example.json benchmark/models.json

# 4. run
python -m benchmark.runner --models benchmark/models.json                 # full, all gates
python -m benchmark.runner --models benchmark/models.json --level l2      # no docker needed
python -m benchmark.runner --models benchmark/models.json --level l1      # offline
python -m benchmark.runner --models benchmark/models.json --tiers 1 --tasks t1_fake_console
python -m benchmark.runner --models benchmark/models.json --max-repairs 3
```

## Output (`benchmark/results/`, git-ignored)

- `results.json` — every attempt: per-layer verdicts, error details, timing
- `summary.csv` — one row per model×task (layer outcomes, first_pass_round)
- `summary.md` — four comparison matrices: layer funnel, repair curve,
  per-category, per-tier
- `configs/` — every generated/repaired config (`…__r0.conf`, `…__r1.conf`, …)

Console example:

```
claude-sonnet-4 (bedrock)  (28 tasks)
  First-attempt layer funnel:
    L1 static    : 92.9%  (n=28)
    L2 dry-run   : 82.1%  (n=28)
    L3 execution : 67.9%  (n=27)
  Repair curve (all enabled gates):
    pass@1       : 67.9%
    pass@<=3     : 89.3%
    pass@<=5     : 92.9%
  By tier (pass@1 / pass@<=5):
    tier 1 (10) : 90.0% / 100.0%
    ...
```

## Methodology notes

- **Determinism**: L2/L3 verdicts are exit-code/liveness based — no LLM judge.
- **Isolation**: each task gets a fresh Orchestrator; CLI state goes to a
  temp `SEATUNNEL_CLI_DATA`; every model sees identical prompts and env.
- **Streaming verdict**: a streaming job that stays alive 60 s without a
  failure pattern (`Job FAILED`, `ErrorCode:[...]`, stack traces) passes;
  early exit or failure output fails. This mirrors how τ-bench-style harnesses
  bound unbounded jobs.
- **Repair loop** reuses `Orchestrator._run_fix` — the exact repair agent the
  product ships — so the curve measures the product's real self-correction
  ability, not a benchmark-only harness.
- Layer funnel is measured on **first attempts only** (uncontaminated by
  repairs); the repair curve measures end-to-end success.
