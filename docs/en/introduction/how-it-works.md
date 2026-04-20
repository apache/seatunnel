---
sidebar_position: 2
---

# How it works

## Overview

SeaTunnel is a distributed multimodal data integration tool with a pluggable architecture. It decouples the connector layer from the execution engine, allowing the same connectors to run on different engines.

This page is the shortest bridge between first-run docs and deeper architecture docs. Read it when you already know SeaTunnel at a high level but still need a practical mental model of how job config, plugins, and engines connect.

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

## Recommended Reading Path

If you are building your first system-level understanding, read in this order:

- [Getting Started Overview](../getting-started/overview.md) for the shortest first-run path
- this page for the execution model in one diagram
- [Engine Overview](../engines/overview.md) for engine selection
- [Architecture Overview](../architecture/overview.md) for the layered system view
- [Core API Design](../architecture/core-api-design.md) for the connector and metadata contracts
- [Transform Plugin System](../architecture/transform-plugin-system.md) if you need to understand dataset wiring and transform behavior

## Core Components

### 1. Connector API

Engine-independent API for developing Source, Transform, and Sink connectors.

| Component | Description |
|-----------|-------------|
| **Source** | Reads data from external systems (databases, files, message queues) |
| **Transform** | Performs data transformations (field mapping, filtering, type conversion) |
| **Sink** | Writes data to target systems |

### 2. Execution Engines

| Engine | Best For |
|--------|----------|
| **SeaTunnel Engine (Zeta)** | Data synchronization, CDC, low resource usage |
| **Apache Flink** | Complex stream processing, existing Flink infrastructure |
| **Apache Spark** | Large-scale batch processing, existing Spark infrastructure |

### 3. Translation Layer

Translates SeaTunnel's unified API to engine-specific implementations, enabling connector reuse across engines.

## Data Flow

```
Source ──▶ [Split] ──▶ Reader ──▶ Transform ──▶ Writer ──▶ Sink
  │                       │                        │
  │                       ▼                        │
  │              Checkpoint/State                  │
  │                       │                        │
  └───────────────────────┴────────────────────────┘
                    Fault Tolerance
```

**Key Features:**
- Parallel reading with split-based distribution
- Exactly-once semantics via distributed snapshots
- Automatic failover and recovery

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

## Job Execution Flow

1. **Parse** - Read and validate job configuration
2. **Plan** - Generate execution plan with parallelism
3. **Schedule** - Distribute tasks to workers
4. **Execute** - Run Source → Transform → Sink pipeline
5. **Monitor** - Track progress, metrics, and checkpoints

## Next Steps

- [Engine Comparison](../engines/overview.md)
- [Getting Started Overview](../getting-started/overview.md)
- [Quick Start With SeaTunnel Engine](../getting-started/locally/quick-start-seatunnel-engine.md)
- [Architecture Overview](../architecture/overview.md)
- [Connector List](../connectors)
