---
sidebar_position: 3
---

# Design

This page explains how the AI CLI turns a natural-language request into a runnable config, and why it is built as a multi-agent pipeline with layered validation rather than a single LLM call.

## Why Not a Single LLM Call

SeaTunnel config generation is a domain-DSL problem with failure modes that a plain "prompt in, config out" call cannot handle reliably:

- **150+ connectors, 20–50 options each** — no model has complete, current knowledge of every option name and type;
- **conditional option dependencies** — e.g. text-format-only options are runtime errors when the format is PARQUET;
- **DAG wiring semantics** — `plugin_output`/`plugin_input` labels must pair exactly across source/transform/sink blocks;
- **mode inference** — CDC sources require STREAMING; bounded sources should not carry streaming-only options.

The CLI therefore grounds the model with authoritative connector metadata, validates every candidate config, and repairs failures with real error feedback.

## Multi-Agent Pipeline

```
User input (natural language)
     │
     ▼
┌─────────────────┐     ┌──────────────────────┐
│  Planner Agent  │───▶│  Connector Knowledge  │
│  (intent +      │◀───│  Base (tools)         │
│   clarification)│     └──────────────────────┘
└────────┬────────┘
         │ structured plan
         ▼
┌─────────────────┐
│  Config Agent   │  Generate HOCON config
└────────┬────────┘
         ▼
┌─────────────────┐     ┌──────────────────────┐
│ Validator Agent │───▶│  Local validation +   │
│                 │     │  engine --check       │
└────────┬────────┘     └──────────────────────┘
         │
    PASS ── Yes ─▶ Output + auto-save
         │
         No (max 3 rounds)
         ▼
┌─────────────────┐
│   Fix Agent     │  Correct errors, re-validate
└─────────────────┘

/check or /run failure at any later point
         ▼
┌─────────────────┐
│  Repair Agent   │  Diagnose real engine/runtime errors, patch config
└─────────────────┘
```

**Planner** classifies intent (new pipeline vs question vs error diagnosis), selects connectors, and asks a clarifying question only when no reasonable default exists. **Config Agent** writes HOCON grounded in per-connector metadata injected into its prompt. **Validator** combines deterministic local checks with an LLM semantic review. **Fix/Repair Agents** receive the actual error text — validation findings during generation, or real engine stack traces after `/check`//`run` — and patch the config rather than regenerating from scratch.

## Connector Knowledge Base

Two-tier resolution keeps option knowledge accurate without hand-maintained prompt text:

1. **Runtime API** — a running SeaTunnel engine's option-rules endpoint (always current);
2. **Bundled metadata** — `connector_metadata.json`, exported from the engine via reflection, shipped with the CLI. Includes required/optional options, conditional groups, and value constraints for sources, sinks, and transforms.

Prompts are assembled per request: only the metadata for the connectors the Planner selected is injected, with conditional options explicitly grouped under their trigger conditions ("only include when `format = text`") — the single most effective measure against invented or misplaced options.

A three-layer generation strategy stacks on top: **Skill SOPs** (scenario playbooks — batch sync, CDC realtime, multi-pipeline, ...) → **Golden Examples** (verified working configs for common pairs) → **Connector Metadata** (authoritative option rules).

## Validation Pipeline

| Phase | Method | Catches |
|-------|--------|---------|
| 1. Local | HOCON syntax, structure, required options vs metadata, routing-label pairing, unresolved `${VAR}` placeholders (field-aware: engine template variables like `${now}` in `file_name_expression` are exempt), security checks | Syntax errors, missing/unknown options, wiring mistakes |
| 2. Engine dry-run | `seatunnel.sh --check` / `--dry-run static` — the engine's real parse path | Plugin loadability, option types, unknown keys, DAG topology |
| 3. Execution | `/run` via REST API or `seatunnel.sh` | Everything runtime: connectivity, schemas, CDC prerequisites |

Each layer is a strict superset of certainty; failures at any layer feed the repair loop with that layer's real error output.

## Design Principles

- **Ground, don't trust**: every connector fact in the prompt comes from engine metadata, not model memory.
- **Validate deterministically, repair with the LLM**: verdicts come from parsers and the engine; the LLM's job is generation and repair, never judging correctness.
- **Real errors are the best prompts**: repair agents receive actual validation output and stack traces. Measured accuracy confirms structured, specific errors repair far better than raw noise (see [benchmark](benchmark.md)).
- **Security by default**: generated configs use `${ENV_VAR}` placeholders for all credentials; session storage redacts secrets; `/remember` rejects sensitive values.

## Measured, Not Assumed

The architecture is evaluated by a [dedicated benchmark](benchmark.md) — 100 tasks, three verdict gates including real execution against Dockerized data sources, across 7 LLMs. Benchmark results directly drive the improvement roadmap (connector-knowledge injection, structured error parsing for the repair loop), and every future change to prompts or metadata can be A/B measured against the same task suite.
