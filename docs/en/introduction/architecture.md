---
sidebar_position: 2
---

# Architecture

## What New Users Should Know First

You do not need to understand every internal module before running SeaTunnel.
For most first-time users, the practical order is:

1. run one job locally,
2. learn the config structure,
3. choose the right connectors,
4. come back here when you want to understand the runtime model better.

SeaTunnel is easiest to understand as a **config-driven pipeline** that runs on a chosen execution engine.

## The Four Building Blocks

```
┌─────────────────────────────────────────────────────────────┐
│                      Job Configuration                       │
│                   (HOCON / SQL / Web UI)                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     SeaTunnel Core                           │
│              (Job Parser, Coordinator, Scheduler)            │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    Source     │────▶│   Transform   │────▶│     Sink      │
│  Connectors   │     │  (Optional)   │     │  Connectors   │
└───────────────┘     └───────────────┘     └───────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Execution Engine                          │
│         SeaTunnel Engine (Zeta) / Flink / Spark              │
└─────────────────────────────────────────────────────────────┘
```

### 1. Config defines the job

The config file tells SeaTunnel what to read, how to transform it, where to write it,
and which engine settings should be used.

### 2. Source, Transform, and Sink define the data path

SeaTunnel jobs use a consistent data path:

* **Source** reads from external systems.
* **Transform** optionally reshapes or filters the data.
* **Sink** writes the result to the target system.

### 3. The engine decides where the job runs

SeaTunnel can execute the same connector-based job model on multiple engines.
Most new users should start with [SeaTunnel Engine (Zeta)](../engines/zeta/about.md), then move to Flink or Spark only when their environment already depends on those platforms.

### 4. Checkpoints and state protect correctness

For long-running or fault-tolerant jobs, the engine coordinates runtime state, checkpoints, and recovery behavior.
You do not need to master those internals to get started, but they are why SeaTunnel can support production-grade synchronization tasks.

## Connector Model

| Component | Description |
|-----------|-------------|
| **Source** | Reads data from external systems (databases, files, message queues) |
| **Transform** | Performs data transformations (field mapping, filtering, type conversion) |
| **Sink** | Writes data to target systems |

The Connector API is engine-independent, which is why the same job model can be reused across multiple execution engines.

## Execution Engines

| Engine | Best For |
|--------|----------|
| **SeaTunnel Engine (Zeta)** | Data synchronization, CDC, low resource usage |
| **Apache Flink** | Complex stream processing, existing Flink infrastructure |
| **Apache Spark** | Large-scale batch processing, existing Spark infrastructure |

## Runtime Flow

```
Source ──▶ [Split] ──▶ Reader ──▶ Transform ──▶ Writer ──▶ Sink
  │                       │                        │
  │                       ▼                        │
  │              Checkpoint/State                  │
  │                       │                        │
  └───────────────────────┴────────────────────────┘
                    Fault Tolerance
```

At a high level, SeaTunnel does the following:

1. parses your config,
2. builds a pipeline from Source to Sink,
3. assigns work to the selected engine,
4. tracks state and progress while the job runs.

## What Changes When You Go Deeper

When you move from onboarding docs to deeper architecture docs, you start learning more specific topics:

* how connectors are translated for different engines,
* how scheduling and resource management work,
* how checkpoints and exactly-once semantics are implemented.

## Module Structure

```
seatunnel/
├── seatunnel-api/           # Core API definitions
├── seatunnel-connectors-v2/ # Source & Sink connectors
├── seatunnel-transforms-v2/ # Transform plugins
├── seatunnel-engine/        # SeaTunnel Engine (Zeta)
├── seatunnel-translation/   # Engine adapters (Flink/Spark)
├── seatunnel-core/          # Job submission & CLI
├── seatunnel-formats/       # Data format handlers
└── seatunnel-e2e/           # End-to-end tests
```

## Next Steps

- [Run your first job](../getting-started/locally/quick-start-seatunnel-engine.md)
- [Learn the config file structure](concepts/config.md)
- [Browse connectors](../connectors/source)
- [Read the full system architecture](../architecture/overview.md)
