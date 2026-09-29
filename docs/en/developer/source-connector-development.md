---
title: Source Connector Development
---

# Source Connector Development

## Goal

This page is the practical entry point for developing a SeaTunnel source connector. It does not replace the low-level API design pages. Instead, it helps contributors translate those APIs into an implementation plan.

If you are building a source connector, read this page first, then move to the deeper architecture references linked below.

## What a Source Connector Must Do

A source connector must solve four problems:

- identify and validate its user-facing options
- describe the output schema
- read data in batch, streaming, or both
- support split assignment and state recovery where parallelism is required

In SeaTunnel, this usually means implementing:

- a source factory
- a `SeaTunnelSource`
- one or more `SourceReader` implementations
- split and enumerator classes when the source is parallel

## Recommended Development Flow

### 1. Start From the User Contract

Before writing any runtime code, define:

- plugin name
- required options
- optional options
- default values
- sample job config

If you cannot explain the connector in a minimal config snippet, the implementation is usually not ready either.

Related docs:

- [Job Configuration Guide](../getting-started/job-configuration-guide.md)
- [Configuration And Option System](../architecture/configuration-and-option-system.md)

### 2. Implement the Factory

The factory is the user-facing entry of the connector. It should:

- expose a stable identifier
- define `OptionRule`
- create the source instance

In practice, the factory is also the bridge between docs, runtime validation, REST metadata exposure, and UI-driven config generation.

### 3. Implement the Source Runtime

For simple sources, a reader may be enough. For scalable or fault-tolerant sources, you also need split and enumerator abstractions.

Typical responsibilities:

- `SeaTunnelSource`: top-level source definition
- `SourceSplitEnumerator`: discover and assign work
- `SourceReader`: read data on workers
- serializers: persist split and enumerator state across network transfer and checkpointing

### 4. Add Packaging and Discovery Metadata

A connector is not complete when the Java code compiles. You also need:

- SPI registration
- plugin mapping
- packaging changes so the connector jar is present in the binary distribution
- plugin dependency layout if isolated dependencies are required

### 5. Document and Test It

A user-visible connector is not considered complete unless:

- `docs/en` and `docs/zh` are updated
- example config matches the code exactly
- unit or E2E tests cover the main reading path

## Design Checklist

Before implementation, answer these questions:

- Is the source bounded, unbounded, or both?
- What is the split unit: file, shard, partition, table range, or something else?
- How does the reader request more work?
- What state is required for recovery?
- How is schema discovered or configured?
- Does the source emit single-table or multi-table output?
- Does the source emit CDC semantics or append-only data?

These answers should drive the class structure, not the other way around.

## Typical Class Layout

For a parallel source, the minimum useful structure often looks like this:

```text
connector-<name>/
  src/main/java/.../source/
    <Name>SourceFactory.java
    <Name>Source.java
    <Name>SourceReader.java
    <Name>SourceSplit.java
    <Name>SourceSplitEnumerator.java
    <Name>SourceConfig.java
```

Depending on complexity, you may also need:

- dialect or client abstraction
- split serializer
- enumerator state class
- reader state helper
- schema discoverer

## Decision Guide

### When a Simple Reader Is Enough

Use a simpler design when:

- the source is single-threaded by nature
- parallelism is not needed
- there is no meaningful split model

### When You Need Splits and an Enumerator

Use the full split-based model when:

- the source can read partitions or ranges in parallel
- failover should reassign unfinished work
- initial discovery and worker-side reading should be separated

This is the default expectation for scalable database, file, queue, and CDC sources.

## Common Source Patterns

### File / Object Storage Source

Common split units:

- file
- block range
- partition directory

Typical concerns:

- file discovery
- schema inference
- checkpointing current file position

### Database Snapshot Source

Common split units:

- primary key range
- partition
- shard

Typical concerns:

- chunk sizing
- query pushdown
- transaction or consistency boundary

### Message Queue Source

Common split units:

- topic partition
- subscription shard

Typical concerns:

- offset management
- watermark or event time
- dynamic partition discovery

### CDC Source

Common split units:

- snapshot chunk
- incremental log split

Typical concerns:

- snapshot to incremental handoff
- source metadata
- schema evolution

Related docs:

- [CDC Pipeline Architecture](../architecture/cdc-pipeline-architecture.md)

## Testing Strategy

At minimum, test these layers:

- option validation
- split generation or discovery
- reader behavior with normal data
- checkpoint or state snapshot behavior
- recovery or split reassignment if the connector is parallel

If the source touches an external system, add or extend E2E coverage when possible.

## Packaging Checklist

Before opening a PR, verify:

- factory registration exists
- connector module is included in build and distribution
- `plugin-mapping.properties` is updated when needed
- doc examples use the exact runtime plugin name
- docs are added in both English and Chinese

## Recommended Reading Path

1. this page for the implementation checklist
2. [Source Architecture](../architecture/api-design/source-architecture.md)
3. [Plugin Discovery and Class Loading](../architecture/plugin-discovery-and-class-loading.md)
4. one existing connector in `seatunnel-connectors-v2/`
5. [How to Create Your Connector](./how-to-create-your-connector.md)
