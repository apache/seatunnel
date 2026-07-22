---
title: Flink Translation Layer
---

# Flink Translation Layer

## Purpose

This page explains how SeaTunnel adapts its engine-independent connector API to Apache Flink.

If you only need the high-level picture, read [Translation Layer](./translation-layer.md) first. This page focuses on the Flink-specific side of that design.

## Why Flink Needs a Translation Layer

SeaTunnel connector authors implement SeaTunnel APIs such as:

- `SeaTunnelSource`
- `SeaTunnelSink`
- `SeaTunnelTransform`

But Flink executes jobs through its own runtime contracts, checkpoint lifecycle, source reader model, and sink interfaces.

The Flink translation layer exists so that:

- connector authors do not write Flink-specific connector code
- Flink jobs can still preserve SeaTunnel semantics
- Flink API changes stay isolated from most connector implementations

## High-Level Mapping

At a conceptual level, the mapping looks like this:

```text
SeaTunnelSource -> FlinkSource adapter -> Flink Source runtime
SeaTunnelSink   -> FlinkSink adapter   -> Flink Sink runtime
SeaTunnel types -> serializer and type adapters -> Flink state and records
```

The translation layer mainly adapts four things:

- lifecycle
- context
- serialization
- checkpoint semantics

## Source-Side Mapping

On the source side, the Flink adapter bridges SeaTunnel's reader/enumerator model to Flink's source runtime.

Typical responsibilities include:

- mapping SeaTunnel boundedness to Flink boundedness
- creating `SourceReader` adapters from SeaTunnel readers
- creating enumerator adapters from SeaTunnel split enumerators
- wrapping split and enumerator state serializers for Flink checkpointing

Why this works well:

- both SeaTunnel and Flink separate coordinator-side and worker-side source responsibilities
- split-based source design maps naturally into Flink's runtime model

Related docs:

- [Source Architecture](./source-architecture.md)

## Sink-Side Mapping

On the sink side, the Flink translation layer adapts SeaTunnel sink contracts into Flink's writer and committer model.

Typical responsibilities include:

- creating Flink writers from `SeaTunnelSink`
- exposing SeaTunnel committer and aggregated committer behavior through Flink-compatible commit paths
- mapping writer state and commit info serializers

This is especially important when the sink uses checkpoint-driven commit semantics.

Related docs:

- [Sink Architecture](./sink-architecture.md)
- [Exactly-Once](../fault-tolerance/exactly-once.md)

## Checkpoint and State Alignment

Flink is one of the main reasons SeaTunnel's source and sink APIs are structured the way they are.

The Flink translation layer must preserve:

- state snapshot timing
- checkpoint completion callbacks
- split and writer state serialization
- commit coordination semantics

If this alignment is wrong, users will usually see the failure as:

- duplicate data
- missing data after recovery
- checkpoint failures
- sink commit inconsistencies

## Context Adapters

Flink runtime contexts expose APIs that do not match SeaTunnel interfaces one-to-one. The translation layer therefore wraps:

- source reader context
- split enumerator context
- sink writer context
- event and metrics channels

This is one of the least visible but most important parts of the translation layer, because it keeps connector implementations from depending on Flink internals.

## Serializer Adapters

Flink requires engine-specific serializer contracts for state, split, and commit information. SeaTunnel provides its own serializers, so the translation layer wraps them into Flink-compatible serializer interfaces.

This matters for:

- checkpoint durability
- versioned state compatibility
- split reassignment and restore

## Strengths of the Flink Path

The Flink translation path is a good fit for SeaTunnel because:

- split-based source design maps well
- checkpoint semantics are mature
- stateful source and sink patterns are well understood

This is one reason SeaTunnel can support complex connector behavior on Flink without asking connector authors to implement Flink-specific code directly.

## Common Trouble Spots

When Flink translation problems appear, they usually concentrate around:

- checkpoint callbacks
- serializer compatibility
- watermark or event-time expectations
- engine-specific config assumptions leaking into connector code

It is important to distinguish:

- a connector bug
- a SeaTunnel API contract issue
- a Flink translation-layer issue

## Code References

Start here if you want to inspect the real implementation:

- `seatunnel-translation/seatunnel-translation-flink/`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/`
- `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/src/main/java/org/apache/seatunnel/translation/flink/source/`

Useful classes include:

- `FlinkSource`
- `FlinkSourceReader`
- `FlinkSourceEnumerator`
- `FlinkSourceReaderContext`
- `FlinkSourceSplitEnumeratorContext`

## Recommended Reading Path

1. [Translation Layer](./translation-layer.md)
2. this page
3. [Source Architecture](./source-architecture.md)
4. [Sink Architecture](./sink-architecture.md)
5. [Exactly-Once](../fault-tolerance/exactly-once.md)
