---
title: Core API Design
---

# Core API Design

## Why This Page Exists

SeaTunnel already has separate pages for source architecture, sink architecture, catalog metadata, and translation. What is still missing is one page that explains how those APIs fit together as a single design.

This page gives that bridge.

## Design Goal

SeaTunnel's core API design has one primary goal:

**connector developers should express data integration logic once, while engines handle execution differences underneath.**

To make that work, the API layer has to do three things at the same time:

- provide stable contracts for connector authors
- carry enough metadata for validation and planning
- stay independent from Flink, Spark, and Zeta-specific runtime details

## The API Stack

At a high level, SeaTunnel's API layer is organized like this:

```text
User Config
   |
   v
Option / OptionRule / ReadonlyConfig
   |
   v
Factory Layer
  - TableSourceFactory
  - TableSinkFactory
  - Transform factory
   |
   v
Runtime Contracts
  - SeaTunnelSource
  - SeaTunnelSink
  - SeaTunnelTransform
   |
   v
Metadata Contracts
  - CatalogTable
  - TableSchema
  - SeaTunnelDataType
  - SchemaChangeEvent
   |
   v
Translation / Engine Runtime
```

The important point is that connector logic sits in the middle: high enough to avoid engine coupling, but rich enough to describe real-world data pipelines.

## The Five Core API Areas

### 1. Configuration Contract

Before a connector can run, SeaTunnel needs a stable way to describe user-facing options and validate them.

The configuration contract is centered on:

- `Option`
- `OptionRule`
- `ReadonlyConfig`

This part of the API matters because it connects:

- documentation
- runtime validation
- plugin discovery metadata
- UI or REST-driven configuration workflows

Related docs:

- [Configuration And Option System](./configuration-and-option-system.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

### 2. Source Contract

The source side is responsible for turning an external system into a stream of `SeaTunnelRow` records, plus schema and state metadata when needed.

Core interfaces:

- `SeaTunnelSource`
- `SourceSplitEnumerator`
- `SourceReader`
- `SourceSplit`

The design separates coordination from execution:

- the enumerator manages discovery and assignment
- the reader performs real data fetching on workers

This makes parallelism, failover, and checkpoint recovery possible without forcing each connector to invent its own execution model.

Related docs:

- [Source Architecture](./api-design/source-architecture.md)

### 3. Sink Contract

The sink side turns processed rows into externally visible side effects.

Core interfaces:

- `SeaTunnelSink`
- `SinkWriter`
- `SinkCommitter`
- `SinkAggregatedCommitter`

The design goal is not only "write data out", but to let a sink clearly define:

- writer-side buffering and prepare logic
- commit coordination
- retry and idempotency behavior
- compatibility with checkpoint-driven recovery

Related docs:

- [Sink Architecture](./api-design/sink-architecture.md)

### 4. Transform Contract

Transforms are the middle layer between source and sink. They operate on SeaTunnel's row and table model rather than on engine-native records.

This gives SeaTunnel a consistent contract for:

- field-level mapping
- filtering
- SQL-like logical projection
- metadata enrichment
- multi-table routing and transformation

The transform contract is what allows a job to remain declarative even when the physical runtime changes underneath.

Related docs:

- [Transform Plugin System](./transform-plugin-system.md)
- [Transforms Catalog](../transforms)

### 5. Metadata Contract

Row processing alone is not enough for a serious integration system. SeaTunnel also needs a portable metadata model that can describe:

- table identity
- schema
- types
- constraints
- partition keys
- schema change events

That is the role of:

- `CatalogTable`
- `TableSchema`
- `Column`
- `SeaTunnelDataType`
- `SchemaChangeEvent`

This metadata layer is essential for:

- sink-side schema validation
- multi-table jobs
- schema evolution
- engine-independent planning

Related docs:

- [CatalogTable and Metadata Management](./api-design/catalog-table.md)

## How the Pieces Work Together

In a typical SeaTunnel job, the API contracts interact in this order:

1. factories parse config and validate options
2. source, transform, and sink instances are created
3. source publishes `CatalogTable` metadata
4. transforms preserve or reshape row and schema information
5. sink validates or derives the write-side table contract
6. translation or native runtime adapts those contracts to the execution engine

This separation is why SeaTunnel can keep connector logic reusable across multiple engines without forcing connector authors to depend on engine-specific APIs.

## Why the API Is Split This Way

### Separation of User Contract and Runtime Contract

User configuration changes more slowly than internal runtime details. By separating `Option` and `OptionRule` from reader and writer logic, SeaTunnel can keep user-facing configuration stable while evolving execution internals.

### Separation of Runtime Contract and Metadata Contract

Rows, schema, and schema changes have different lifecycles. A connector may read rows continuously, while metadata changes only occasionally. Keeping those contracts distinct makes the system easier to reason about and extend.

### Separation of Logical API and Engine Translation

If connector implementations were written directly against Flink or Spark APIs, every connector would need multiple engine-specific versions. The SeaTunnel API layer avoids that duplication.

Related docs:

- [Translation Layer](./api-design/translation-layer.md)

## Design Questions for Contributors

When adding or reviewing an API-related change, check these questions first:

- Is this change user-facing or only runtime-facing?
- Does it belong in `OptionRule`, runtime APIs, or metadata APIs?
- Will it affect all engines equally, or only one engine adapter?
- Is the option name stable enough to become public contract?
- Does the change preserve backward compatibility for connector authors and users?

These questions matter because API drift is harder to unwind than implementation drift.

## Common Misunderstandings

### "Source and sink APIs are enough"

Not really. Without `CatalogTable`, `SeaTunnelDataType`, and schema change events, connectors would have no engine-independent way to express table metadata and schema evolution.

### "Transform is only row-level mapping"

Not always. In SeaTunnel, transform logic may also need to preserve or reshape metadata, especially in multi-table and CDC pipelines.

### "Translation is just an adapter layer"

It is an adapter layer, but it is also a design boundary. It keeps connector authors from depending on engine internals and limits how much engine-specific behavior leaks into connector code.

## Recommended Reading Path

1. this page for the full API map
2. [Configuration And Option System](./configuration-and-option-system.md)
3. [Source Architecture](./api-design/source-architecture.md)
4. [Sink Architecture](./api-design/sink-architecture.md)
5. [Transform Plugin System](./transform-plugin-system.md)
6. [CatalogTable and Metadata Management](./api-design/catalog-table.md)
7. [Translation Layer](./api-design/translation-layer.md)
