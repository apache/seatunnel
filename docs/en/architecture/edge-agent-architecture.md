---
title: Edge Agent Architecture
---

# Edge Agent Architecture

## 1. Overview

### 1.1 Problem Background

In many production networks, the Zeta cluster cannot directly access edge-local data (host files, private app logs, local event channels). A dedicated edge collector process is needed to:

- read local records close to the source,
- tolerate intermittent network connectivity,
- deliver data into the running SeaTunnel pipeline without embedding engine workers at edge sites.

### 1.2 Design Goals

SeaTunnel Edge Agent (Phase 1) is designed with:

1. **Standalone Deployment**: independent runtime and packaging with `bin/`, `conf/`, and `lib/` under the Edge Agent install root (see Packaging and Operations), separate from engine worker lifecycle.
2. **Durable Outbound Buffering**: SQLite WAL queue with explicit state transitions.
3. **Protocol Alignment with Zeta**: reuse EdgeSocket line protocol and commit polling contract.
4. **Simple Operations**: YAML-based config, start/stop/status scripts, deterministic runtime loop.
5. **Incremental Evolution**: preserve clear boundaries for future parallel senders and richer reliability semantics.

### 1.3 Architecture Positioning

| Aspect | Edge Agent | In-Engine Source |
|--------|------------|------------------|
| Runtime location | Edge host process | Zeta worker task |
| Input access | Local files/logs/events on edge host | Data sources reachable by worker |
| Outbound durability | SQLite WAL in agent | Engine checkpoint/state mechanisms |
| Transport | EdgeSocket text protocol over TCP | Internal task data flow |
| Primary role | Edge collection + forwarding | Pipeline execution |

## 2. Overall Architecture

### 2.1 Logical Topology

```
┌───────────────────────────────────────────────────────────────┐
│                        Edge Host                              │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                 Edge Agent Process                     │  │
│  │  • AgentInput (file/log/event)                         │  │
│  │  • RecordBatchAccumulator                              │  │
│  │  • SqliteOutboundWal (PENDING/SENDING/ACKED)          │  │
│  │  • EdgeTransportClient                                 │  │
│  └─────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
                               │
                               │ (TCP line protocol)
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                  SeaTunnel Engine (Zeta)                     │
│                                                               │
│  SeaTunnelClient discovery (jobId -> task-group hosts)       │
│               +                                               │
│  EdgeSocket Source ingress (__AUTH__/__BATCH__/__COMMIT__)   │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 Module Architecture

| Module | Artifact | Responsibility |
|--------|----------|----------------|
| `seatunnel-edge-agent` | `seatunnel-edge-agent` (`pom`) | Parent aggregator and distribution entry |
| `seatunnel-edge-agent-core` | `seatunnel-edge-agent-core` | Runtime bootstrap, YAML load/validate, accumulator loop, WAL lifecycle |
| `seatunnel-edge-agent-transport` | `seatunnel-edge-agent-transport` | EdgeSocket protocol client, discovery/reconnect policy |
| `seatunnel-edge-agent-connector` | `seatunnel-edge-agent-connector` | Built-in `AgentInput` implementations and NDJSON normalization |
| `seatunnel-dist` (standard assemblies) | `seatunnel-dist` (`pom`) | `assembly-bin.xml` / `assembly-bin-ci.xml` / `assembly-src.xml` package module-level `bin/`, `conf/`, and jars |

## 3. Runtime Execution Model

### 3.1 Startup Flow

```mermaid
sequenceDiagram
    participant Main as EdgeAgentMain
    participant Boot as EdgeAgentBootstrap
    participant WAL as SqliteOutboundWal
    participant ST as SeaTunnelClient
    participant ET as EdgeTransportClient

    Main->>Boot: resolve config path + start()
    Boot->>WAL: open(); recoverStaleSending()
    Boot->>ST: create SeaTunnelClient (cluster-name/addresses)
    Boot->>ET: open() (discovery + auth)
    Boot->>Boot: open all AgentInput bindings
    Boot->>Boot: enter main loop
```

### 3.2 Main Loop

Per iteration:

1. Poll each input up to `queue.poll-batch-size`.
2. Accumulate records until `bulk-max-size` or `flush-interval-ms`.
3. Persist flushed slices into WAL as `PENDING`.
4. Claim a sending slice (`PENDING -> SENDING`) with retry-budget filter.
5. Send one merged NDJSON batch and wait commit ACK.
6. On success mark rows `ACKED`; on failure revert to `PENDING` and increment `attempts`.

### 3.3 Runtime State Transitions (WAL rows)

```
PENDING
  │ claimSendingBatch()
  ▼
SENDING
  │ send + commit ACK success
  ├──────────────► ACKED
  │
  └ send failure / timeout / restart recovery
                 ▼
               PENDING (attempts + 1)
```

## 4. Discovery and Protocol Model

### 4.1 Endpoint Discovery

`EdgeTransportClient` does not hardcode worker endpoints. It queries `SeaTunnelClient.getJobTaskGroupAddresses(jobId)` through `JobTaskGroupAddressesLookup`, parses host list, then combines hosts with `output.port` as EdgeSocket ingress targets.

### 4.2 Wire Protocol

| Phase | Agent -> Engine | Engine -> Agent |
|------|------------------|-----------------|
| Auth | `__AUTH__:<token>` | `ACK` / `AUTH_FAILED` |
| Batch push | `__BATCH__:<batchId>:<payload>` | `RECEIVED` / `RETRY` |
| Commit poll | `__COMMIT__:<batchId>` | `PENDING` / `RETRY` / `ACK:<n>` |

ACK completion condition: `n >= batchId`.

### 4.3 Reconnect / Rediscover Policy

On I/O or auth failure:

1. invalidate current socket session,
2. rediscover task-group addresses,
3. rotate endpoint candidates,
4. reconnect + re-authenticate with backoff and bounded cycles.

## 5. Durability and Failure Handling

### 5.1 WAL Guarantees

- Agent-side durable queue lives in SQLite (`outbound_records`).
- `recoverStaleSending()` protects crash window between claim and ACK.
- `retry.max-attempts` bounds replay pressure by excluding over-attempt rows from new claims.

### 5.2 Failure Scenarios

| Failure | Behavior |
|--------|----------|
| Agent process crash | WAL `SENDING` rows restored to `PENDING` on restart |
| Temporary network outage | Send failure triggers revert + backoff + reconnect/rediscover |
| Worker address drift | Next rediscovery refreshes endpoint candidates |
| Graceful shutdown | In-memory accumulator is drained into WAL before exit |

### 5.3 Delivery Semantics (Phase 1)

Phase 1 semantics are bounded by the ACK contract:

- row removal from retry set happens only after `ACK:<batchId>` coverage,
- duplicates remain possible across crash/retry boundaries,
- downstream idempotency is recommended for strict deduplication.

## 6. Configuration and Inputs (Phase 1)

### 6.1 `agent.yaml` Surface

- `inputs`: ordered sources (`file` / `log` / `event`) with logical ids.
- `output`: cluster bootstrap (`cluster-name`, `cluster-addresses`) + delivery identity (`job-id`, `auth-token`, `port`) + timeouts.
- `queue`: SQLite path and poll batch size.
- `batch`: in-memory flush thresholds.
- `retry`: WAL send retry budget and sleep interval.

Example: [`conf/agent.yaml`](../../../seatunnel-edge-agent/conf/agent.yaml)

### 6.2 Input Behaviors

| Type | Behavior |
|------|----------|
| `file` | Read non-empty NDJSON lines from configured files in order |
| `log` | Tail-style read (or from beginning) from single log file |
| `event` | File-backed preload mode or memory-injection mode (empty paths) |

## 7. Packaging and Operations

### 7.1 Install root and paths

In the source tree, launcher scripts and the sample YAML are authored under **`seatunnel-edge-agent/bin`** and **`seatunnel-edge-agent/conf`**. The **`seatunnel-dist`** standard assemblies (`assembly-bin*.xml`) copy those into the distribution package.

Relative paths such as `bin/seatunnel-edge-agent.sh` and `conf/agent.yaml` refer to the **install root**: the directory that parents `bin/`, `conf/`, and `lib/` when you run the scripts — typically the top-level folder after unpacking `apache-seatunnel-edge-agent-*-bin.tar.gz`, or a scratch directory where you placed copies of `bin/`, `conf/`, and built jars under `lib/` for development. They are not the SeaTunnel engine’s top-level `bin/` or `config/` at the repository root unless you intentionally reuse those paths.

### 7.2 Distribution Artifacts

- Child jars: `core`, `transport`, `connector`.
- Binary package (from `seatunnel-dist` `assembly-bin*.xml`) includes:
  - `bin/`
  - `conf/`
  - `lib/`

### 7.3 Scripted Lifecycle

- Unix: `bin/seatunnel-edge-agent.sh start|stop|status`
- Windows: `bin/seatunnel-edge-agent.cmd start|stop|status`

Both resolve defaults from the install root (parent of `bin/`). They support environment overrides for config, PID file, and log path.

Direct execution of `EdgeAgentMain` (without the scripts) resolves `./conf/agent.yaml` relative to the JVM working directory; align your cwd or pass `--config` / `EDGE_AGENT_CONFIG` accordingly.

## 8. Related Resources

- [Architecture Overview](./overview.md)
- [Engine Architecture](./engine/engine-architecture.md)
- [Checkpoint Mechanism](./fault-tolerance/checkpoint-mechanism.md)
