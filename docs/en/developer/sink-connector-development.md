---
title: Sink Connector Development
---

# Sink Connector Development

## Goal

This page is the practical entry point for contributors implementing a SeaTunnel sink connector. It focuses on the decisions that matter before writing code and during review.

## What Makes Sink Development Different

Sink connectors are usually harder to get right than sources because correctness depends on external side effects.

A sink connector must make its guarantees explicit:

- append-only, at-least-once, or stronger semantics
- idempotent or transactional commit behavior
- handling of insert, update, and delete events
- schema compatibility with the target system

If these guarantees are not clearly defined, the connector may appear to work in simple demos while failing under retry or recovery.

## Recommended Development Flow

### 1. Define the Write Contract First

Before implementing a sink, specify:

- target system model: append, overwrite, upsert, or transactional table commit
- primary key requirements
- delete support
- schema evolution expectations
- failure and retry behavior

This should be reflected in both the connector docs and the code.

### 2. Define Stable Options

The sink factory should define:

- required options
- optional options
- default values
- mutually exclusive or bundled rules

Do not treat option names as temporary. They are user-facing contracts.

Related docs:

- [Configuration And Option System](../architecture/configuration-and-option-system.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)

### 3. Choose the Commit Model

SeaTunnel sink design supports several levels of sophistication:

- writer only
- writer + committer
- writer + committer + aggregated committer

The right choice depends on the target system.

Use a simpler model only when the consistency tradeoff is acceptable and documented.

### 4. Implement Runtime + Packaging

A complete sink contribution normally includes:

- sink factory
- `SeaTunnelSink`
- `SinkWriter`
- optional `SinkCommitter`
- optional `SinkAggregatedCommitter`
- packaging and discovery metadata

### 5. Verify Recovery Behavior

Do not stop after a happy-path write test. Verify what happens when:

- `prepareCommit` runs and the task fails
- commit is retried
- the sink sees duplicate commit requests
- the target table schema changes

## Design Checklist

Before coding, answer these questions:

- Is the sink append-only or CDC-aware?
- Does the target system support idempotent upsert?
- Is delete propagation required?
- Does exactly-once style delivery depend on transactions?
- What state must be restored after checkpoint recovery?
- What happens if the same commit is replayed twice?

## Typical Class Layout

```text
connector-<name>/
  src/main/java/.../sink/
    <Name>SinkFactory.java
    <Name>Sink.java
    <Name>SinkWriter.java
    <Name>SinkConfig.java
```

Depending on the sink semantics, you may also need:

- `<Name>CommitInfo`
- `<Name>WriterState`
- `<Name>SinkCommitter`
- `<Name>SinkAggregatedCommitter`
- schema or table helper classes

## Commit Model Guide

### Writer Only

Use only a writer when:

- at-least-once or weaker semantics are acceptable
- the target system has natural idempotency
- no centralized commit coordination is required

### Writer + Committer

Use a committer when:

- each writer prepares work independently
- commit can happen per writer or per partition
- retries must be centralized and explicit

### Writer + Aggregated Committer

Use an aggregated committer when:

- the sink needs a single table-level or global commit point
- all writer outputs must be combined before final visibility
- failure handling must be coordinated globally

This model is especially important for table-oriented sinks and strong consistency use cases.

## CDC-Aware Sink Design

If the sink accepts CDC input, define the mapping very clearly:

- insert -> ?
- update -> ?
- delete -> ?

Also specify:

- whether the sink requires a primary key
- whether schema changes are applied automatically
- whether unsupported row kinds are rejected, ignored, or transformed upstream

Related docs:

- [CDC Pipeline Architecture](../architecture/cdc-pipeline-architecture.md)
- [Sink Architecture](../architecture/api-design/sink-architecture.md)

## Common Pitfalls

### Doing Real External Commit in `prepareCommit`

`prepareCommit` should not silently become the final commit point unless the sink contract is intentionally simpler and documented.

### Non-Idempotent Retry Behavior

If commit may run again after failure, duplicate side effects must not corrupt the target system.

### Hiding Semantic Limits

If the sink cannot support deletes, schema evolution, or exactly-once style recovery, say so explicitly in the docs.

## Testing Strategy

At minimum, cover:

- option validation
- writer behavior
- commit preparation behavior
- retry and idempotency behavior
- recovery from checkpoint or restart

If the sink is important for production usage, E2E coverage is strongly preferred.

## Packaging Checklist

Before opening a PR, verify:

- factory registration exists
- packaging includes the connector
- plugin mapping and dependency layout are correct when required
- docs examples match the real plugin identifier
- English and Chinese docs are both updated

## Recommended Reading Path

1. this page for the design checklist
2. [Sink Architecture](../architecture/api-design/sink-architecture.md)
3. [Exactly-Once](../architecture/fault-tolerance/exactly-once.md)
4. [Plugin Discovery and Class Loading](../architecture/plugin-discovery-and-class-loading.md)
5. [How to Create Your Connector](./how-to-create-your-connector.md)
