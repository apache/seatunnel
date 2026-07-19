---
title: Spark Translation Layer
---

# Spark Translation Layer

## Purpose

This page explains how SeaTunnel adapts its connector APIs to Apache Spark.

If you only need the high-level architecture first, read [Translation Layer](./translation-layer.md). This page focuses on the Spark-specific path.

## Why Spark Translation Is Different

Spark is not just "another engine" in the same shape as Flink. Its execution model, datasource interfaces, and commit lifecycle differ in important ways.

That means the Spark translation layer must do more than rename interfaces. It must reinterpret SeaTunnel contracts in a Spark-compatible execution model.

## The Main Design Goal

The Spark translation layer tries to preserve SeaTunnel semantics while adapting them to Spark-native concepts such as:

- datasource readers
- input partitions
- internal rows
- datasource writers and commit messages

The point is not to make Spark look identical to Flink. The point is to keep connector authors shielded from Spark-specific complexity.

## High-Level Mapping

Conceptually, the Spark path looks like this:

```text
SeaTunnelSource -> Spark source adapter -> Spark datasource runtime
SeaTunnelSink   -> Spark sink adapter   -> Spark datasource writer runtime
SeaTunnel schema/types -> Spark schema/types -> InternalRow execution
```

This translation is especially important in three places:

- source partition planning
- row and schema conversion
- sink commit and abort behavior

## Source-Side Adaptation

On the source side, Spark translation typically needs to:

- expose schema in Spark's expected shape
- plan partitions from SeaTunnel split information
- create per-partition readers
- convert SeaTunnel output into Spark `InternalRow`

Compared with Flink, Spark tends to emphasize planned partitions and reader execution more than a continuously active enumerator/runtime coordinator model.

That difference shapes the adapter design.

Related docs:

- [Source Architecture](./source-architecture.md)
- [Table Schema and Type System](../table-schema-and-type-system.md)

## Sink-Side Adaptation

On the sink side, Spark translation maps SeaTunnel sink behavior into Spark datasource writer contracts.

Typical responsibilities include:

- creating writer factories
- carrying commit messages from executors
- coordinating commit and abort paths
- mapping retry semantics into Spark-compatible behavior

This becomes especially important when the sink is not append-only and needs idempotent or transactional behavior.

Related docs:

- [Sink Architecture](./sink-architecture.md)
- [Exactly-Once](../fault-tolerance/exactly-once.md)

## Schema and Row Conversion

Spark translation depends heavily on schema conversion because Spark executes through its own strongly defined row and schema model.

The translation layer therefore needs to map:

- `CatalogTable` / `TableSchema`
- `SeaTunnelDataType`
- `SeaTunnelRow`

into Spark concepts such as:

- `StructType`
- Spark SQL data types
- `InternalRow`

This is one of the most sensitive boundaries in the Spark path, especially for:

- decimals
- timestamps
- nested types
- nullability

## Version Split

Spark translation in SeaTunnel is version-aware. Spark 2.4 and Spark 3.x do not expose identical datasource APIs, so SeaTunnel keeps separate modules and adapters for major Spark lines.

This matters because a connector may behave correctly at the SeaTunnel API layer while still requiring Spark-version-specific adapter behavior underneath.

## Commit and Recovery Concerns

Spark sink execution has its own writer and commit message model. The translation layer has to bridge SeaTunnel writer and committer semantics into that model without losing:

- idempotency expectations
- failure handling behavior
- abort correctness
- consistency guarantees promised by the connector

If this bridge is weak, users usually experience:

- duplicate side effects
- broken abort paths
- writer commit mismatches

## Common Trouble Spots

Spark translation issues tend to cluster around:

- schema conversion mismatches
- `InternalRow` conversion
- datasource writer commit behavior
- differences between Spark 2.4 and Spark 3.x adapters

These problems can be subtle because the source of failure may look like a connector issue while the real bug is inside the translation layer.

## Code References

Useful entry points:

- `seatunnel-translation/seatunnel-translation-spark/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-common/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-2.4/`
- `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-3.3/`

Representative classes include:

- Spark source provider implementations
- `SparkSink`
- `SparkDataSourceWriter`
- `SeaTunnelInputPartitionReader`

## Recommended Reading Path

1. [Translation Layer](./translation-layer.md)
2. this page
3. [Source Architecture](./source-architecture.md)
4. [Sink Architecture](./sink-architecture.md)
5. [Table Schema and Type System](../table-schema-and-type-system.md)
