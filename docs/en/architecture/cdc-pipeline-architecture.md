---
title: CDC Pipeline Architecture
---

# CDC Pipeline Architecture

## Why This Page Exists

SeaTunnel already has many connector pages that explain how to configure specific CDC sources and sinks. What is still easy to miss is the end-to-end architecture behind a CDC pipeline:

- how snapshot and incremental phases are combined
- how change events are represented in the SeaTunnel row model
- how schema changes and table metadata propagate through the pipeline
- how checkpoint, state recovery, and sink commit semantics work together

This page gives that system view.

## Where CDC Fits in SeaTunnel

A CDC job is still a normal SeaTunnel job:

```text
Source -> Transform -> Sink
```

The difference is that the source emits change events instead of only append-only rows. A CDC pipeline usually has these characteristics:

- the source starts with a snapshot or other bootstrap phase
- the source then switches to incremental log reading
- rows carry row kind and source metadata
- the pipeline may propagate schema changes
- the sink must decide how to apply insert, update, and delete events

CDC is not a separate execution system. It is a specialized dataflow built on top of the same Source API, checkpointing, and engine scheduling model.

## High-Level Dataflow

```text
Database / Log Source
        |
        v
CDC Source
  - snapshot split discovery
  - incremental split discovery
  - offset tracking
        |
        v
SeaTunnelRow + RowKind + metadata
        |
        v
Transforms
  - routing
  - filtering
  - metadata extraction
  - rowkind conversion
        |
        v
CDC-aware Sink
  - upsert / delete handling
  - transactional or idempotent commit
```

## Core Building Blocks

### Snapshot + Incremental Reading

Most relational CDC connectors do not start directly from the changelog stream. They first need a consistent snapshot of existing data, then continue with incremental changes.

The common pattern is:

1. split large tables into snapshot chunks
2. assign those chunks to parallel readers
3. track a handoff point between snapshot and incremental reading
4. continue from the database log or change stream

This is why CDC sources usually have more complex enumerator and split state than ordinary batch sources.

### Row Model and Change Semantics

CDC data is carried through the pipeline as `SeaTunnelRow`, but the row is no longer interpreted as append-only by default. It may represent:

- insert
- update before / update after
- delete

Some transforms and sinks preserve this semantics directly. Others convert it into append-only data, for example when writing to systems that do not support update or delete natively.

Related docs:

- [Catalog Table](./api-design/catalog-table.md)
- [RowKindExtractor Transform](../transforms/rowkind-extractor.md)
- [Metadata Transform](../transforms/metadata.md)

### Multi-Table and Schema Evolution

CDC is often used for full-database or multi-table synchronization. In those cases the source may emit records from many tables, along with table identity and schema change events.

SeaTunnel uses table metadata to support:

- multi-table routing
- sink-side table creation or lookup
- schema evolution decisions
- placeholder substitution in sink options

Related docs:

- [Multi-Table](./features/multi-table.md)
- [Catalog Table](./api-design/catalog-table.md)
- [Schema Evolution Configuration](../introduction/configuration/schema-evolution.md)

## Execution Phases

### Job Startup

At startup, SeaTunnel parses the job config, validates connector options, discovers the required plugins, and creates the logical and physical plan. CDC jobs follow the same startup path as other jobs.

### Snapshot Phase

During the snapshot phase, the source enumerator generates snapshot splits. Readers process those splits in parallel and keep per-split progress in state.

This phase affects:

- startup cost
- initial consistency boundary
- recovery cost when failures happen during bootstrap

### Incremental Phase

Once the snapshot phase is complete, the source moves to incremental reading. At this point, the source tracks database-specific offsets or positions such as binlog positions, GTID, LSN, or equivalent cursors.

### Sink Application

The sink decides how to materialize change events. Common patterns are:

- append-only write after converting CDC events upstream
- key-based upsert
- delete propagation
- two-phase commit or idempotent commit for exactly-once style delivery

## Checkpoint and Recovery

Checkpoint is critical for CDC because the system must recover both:

- source-side progress, including split and offset state
- sink-side commit state, if the sink participates in exactly-once style coordination

In practice:

- the source snapshots split state and incremental offsets
- the engine persists checkpoint metadata
- on recovery, readers and enumerators rebuild their state from the last successful checkpoint
- sinks restore uncommitted or retryable commit information if supported

Related docs:

- [Checkpoint Mechanism](./fault-tolerance/checkpoint-mechanism.md)
- [Exactly-Once](./fault-tolerance/exactly-once.md)

## Source-Side Architecture

SeaTunnel CDC sources are built on the same Source API used by other connectors, but typically rely on a richer split model.

Typical source-side components include:

- `SeaTunnelSource`
- `SourceSplitEnumerator`
- `SourceReader`
- snapshot split state
- incremental split state
- database-specific dialect or offset abstraction

Common responsibilities:

- discover snapshot chunks
- assign and reassign splits
- track completed snapshot splits
- keep incremental offsets durable through checkpoint
- surface schema and metadata to downstream operators

Related docs:

- [Source Architecture](./api-design/source-architecture.md)
- [Configuration And Option System](./configuration-and-option-system.md)

## Sink-Side Architecture

CDC sinks need a clear contract for update and delete handling. When reviewing or designing a sink for CDC, check these questions first:

- Does the sink support primary-key-based upsert?
- Are delete events preserved, ignored, or transformed?
- Is commit idempotent?
- Does the sink participate in checkpoint-driven commit?
- How are schema changes handled?

Related docs:

- [Sink Architecture](./api-design/sink-architecture.md)
- connector-specific sink pages under [Sink Connectors](../connectors/sink)

## Operational Concerns

CDC pipelines are more sensitive than append-only batch jobs. The main operational concerns are:

- source log retention and lag
- snapshot duration for large tables
- checkpoint interval and checkpoint size
- sink commit latency
- schema evolution compatibility
- plugin dependency isolation across cluster nodes

When a CDC job behaves incorrectly, the first places to inspect are:

- source connector logs
- checkpoint status
- sink commit logs
- REST API or Web UI job status

Related docs:

- [REST API and Web UI](../engines/zeta/rest-api-and-web-ui.md)
- [Plugin Discovery and Class Loading](./plugin-discovery-and-class-loading.md)
- [Connector Isolated Dependency Loading Mechanism](../connectors/connector-isolated-dependency.md)

## Code References

If you want to study the implementation rather than only the docs, start here:

- `seatunnel-connectors-v2/connector-cdc/connector-cdc-base/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-sqlserver/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-oracle/`
- `seatunnel-connectors-v2/connector-cdc/connector-cdc-mongodb/`

The most useful classes to inspect first are usually:

- `IncrementalSource`
- `IncrementalSourceEnumerator`
- `HybridSplitAssigner`
- `SnapshotSplitAssigner`
- `IncrementalSourceReader`

## Recommended Reading Path

1. this page for the whole pipeline view
2. [Source Architecture](./api-design/source-architecture.md)
3. [Catalog Table](./api-design/catalog-table.md)
4. [Checkpoint Mechanism](./fault-tolerance/checkpoint-mechanism.md)
5. one concrete CDC connector page such as MySQL, Postgres, or SQL Server
