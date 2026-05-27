---
sidebar_position: 1
title: About
---

# About Edge Agent

Edge Agent is the operational surface for SeaTunnel Edge Agent — a lightweight collector on edge hosts where source data is only reachable locally (log files, host-local paths, and similar). The agent buffers outbound records in a SQLite WAL and forwards batches to a running SeaTunnel job through the EdgeSocket line protocol.

Edge Agent is not a replacement for SeaTunnel Engine. Typical topology:

```text
  Edge host                         SeaTunnel Engine cluster
  +------------------+              +---------------------------+
  | Edge Agent       |  EdgeSocket  | Job with EdgeSocket Source|
  | (this module)    | -----------> | (ingest + pipeline)       |
  +------------------+              +---------------------------+
```

## When to use Edge Agent

- Sources exist only on edge machines (for example /var/log, application log directories).
- You want a small, long-lived daemon with local durability before network send.
- The downstream pipeline uses [EdgeSocket Source](../connectors/source/EdgeSocket.md) on the engine side.

## When not to use Edge Agent

- Sources are reachable directly from the engine cluster (use connector sources on the engine instead).
- You need full SeaTunnel transform/sink orchestration on the edge host (deploy engine workers or another runtime there).

## Glossary

| Term | Definition |
|------|------------|
| WAL | The local SQLite outbound queue durability mechanism in Edge Agent, used to persist and retry outbound records until RECEIVED. |
| BEST_EFFORT | Delivery behavior in this release: persist to local WAL and retry until RECEIVED; duplicate delivery can occur. |
| WAL row states | PENDING (ready), SENDING (in-flight), ACKED (acknowledged), DEAD (retry limit exceeded). |
| Engine response codes | ACK, AUTH_FAILED, REJECTED, RECEIVED, RETRY, QUEUE_FULL, DECRYPT_FAILED. |

## Recommended reading order

| Phase | Document | Description |
|-------|----------|-------------|
| Try it out | [Quick Start](quick-start.md) | Local test → connect to Engine |
| Install | [Download](download.md) / [Deployment Guide](deployment-guide.md) | Package download and production setup |
| Configure | [Input Configuration](input-configuration.md) / [Output Configuration](output-configuration.md) | Scenario-based YAML examples |
| Reference | [Configuration Reference](configuration.md) | Full agent.yaml parameter tables |
| Deep dive | [Architecture Overview](./architecture-overview.md) | Design, reliability and Engine boundary |
| Engine side | [EdgeSocket Source](../connectors/source/EdgeSocket.md) | Engine-side receive protocol |
| Operate | [Operations](operations.md) | Start/stop, logs, troubleshooting |

