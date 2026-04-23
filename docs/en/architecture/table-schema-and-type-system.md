---
title: Table Schema and Type System
---

# Table Schema and Type System

## Why This Page Exists

SeaTunnel already has detailed pages for schema configuration and for `CatalogTable` metadata. What is still missing is a single system-level explanation of how table schema and the type system work together across connectors, transforms, and engines.

This page fills that gap.

## The Core Idea

SeaTunnel needs a schema and type model that can do all of the following at once:

- describe external tables and records precisely enough for real data movement
- stay independent from a specific execution engine
- support both static schemas and runtime schema evolution
- remain usable across source, transform, and sink contracts

That model is centered on:

- `CatalogTable`
- `TableSchema`
- `Column`
- `SeaTunnelDataType`
- `SchemaChangeEvent`

## Where Schema Appears in a SeaTunnel Job

Schema is not only a source-side concern. It participates in the entire pipeline.

```text
User config or source discovery
        |
        v
CatalogTable / TableSchema
        |
        v
Source output contract
        |
        v
Transform planning and validation
        |
        v
Sink compatibility and write contract
        |
        v
Engine translation and runtime execution
```

This is why schema mistakes often show up as connector issues, transform issues, and sink issues at different times.

## The Main Building Blocks

### CatalogTable

`CatalogTable` is the top-level metadata object. It carries table identity and the schema needed by the pipeline.

It typically includes:

- table identifier
- table schema
- options
- partition keys
- comments and related metadata

### TableSchema

`TableSchema` describes the logical structure of a table:

- columns
- primary key
- constraint keys

It is the core contract used by connectors and transforms when they need a portable description of tabular data.

### Column

Each column contributes:

- name
- type
- nullability
- default value
- comment

### SeaTunnelDataType

`SeaTunnelDataType` is SeaTunnel's portable type system. It allows connectors to describe fields without coupling that description to Flink, Spark, JDBC, Avro, or a single database dialect.

## Why a Dedicated Type System Is Necessary

SeaTunnel integrates many heterogeneous systems. A single job may read from one type system and write to another:

- JDBC types
- Kafka / Avro types
- JSON payloads
- file schemas
- CDC metadata and row kinds

If SeaTunnel did not normalize these into its own model, every transform and sink would need engine-specific and connector-specific type logic.

The dedicated type system reduces that fragmentation.

## Common Type Categories

SeaTunnel's type system needs to support more than primitive values.

### Primitive Types

Typical primitive types include:

- string
- boolean
- tinyint / smallint / int / bigint
- float / double / decimal
- bytes
- date / time / timestamp / timestamp with timezone-style semantics where supported

### Complex Types

The schema model also needs to support nested structures, especially for semi-structured data and CDC payloads:

- array
- map
- row

These complex types matter because modern data integration pipelines rarely remain flat from end to end.

## Schema Sources

In SeaTunnel, schema may come from more than one place.

### Source-Discovered Schema

Some connectors can derive schema directly from the external system, such as:

- relational databases
- catalogs
- metadata services

### User-Declared Schema

Some systems do not expose a strong schema, or users may want to override or supplement it. In those cases, SeaTunnel supports user-defined schema configuration.

Related docs:

- [Schema Feature](../introduction/concepts/schema-feature.md)

### Propagated or Derived Schema

Transforms may preserve the incoming schema, reduce it, rename fields, or produce a new derived schema.

This means schema is not just "loaded once". It is often part of the job's evolving logical contract.

## Schema in Source, Transform, and Sink

### Source Side

A source uses schema to define what downstream operators should expect.

Typical source-side uses:

- producing `CatalogTable`
- describing table identity
- exposing multi-table metadata
- mapping external types into `SeaTunnelDataType`

### Transform Side

A transform may:

- preserve schema
- project a subset of fields
- rename columns
- derive new columns
- map schema change events

In other words, transform logic is not only row-level logic. It is often schema logic too.

### Sink Side

A sink uses schema to determine whether the incoming data can be written safely.

Typical sink-side checks:

- field existence
- type compatibility
- primary key expectations
- partitioning metadata
- schema evolution behavior

## Multi-Table and CDC Implications

Schema becomes especially important in two kinds of jobs.

### Multi-Table Jobs

When a source emits multiple tables, SeaTunnel needs a strong table identity and schema model so that routing and sink application remain correct.

### CDC Jobs

In CDC pipelines, schema is not static. The system may need to propagate schema changes at runtime, which is why schema handling is tightly connected to:

- `SchemaChangeEvent`
- checkpoint and recovery
- sink-side compatibility logic

Related docs:

- [CDC Pipeline Architecture](./cdc-pipeline-architecture.md)
- [CatalogTable and Metadata Management](./api-design/catalog-table.md)

## Type Mapping Boundaries

The hardest type-system problems usually happen at system boundaries.

High-risk areas include:

- decimal precision and scale
- timestamp semantics and timezone interpretation
- nested row / map / array compatibility
- binary representation
- nullability assumptions

These are the places where "it compiled" does not guarantee "it is safe in production."

## Schema Evolution

A useful schema system must support both stability and controlled change.

SeaTunnel's schema model supports evolution-related workflows such as:

- add column
- drop column
- modify column
- propagate schema changes downstream

This matters most in:

- CDC pipelines
- lakehouse and warehouse ingestion
- long-running jobs whose schema changes over time

Related docs:

- [Schema Evolution Configuration](../introduction/configuration/schema-evolution.md)

## Recommended Reading Path

1. this page for the system view
2. [CatalogTable and Metadata Management](./api-design/catalog-table.md)
3. [Schema Feature](../introduction/concepts/schema-feature.md)
4. [Configuration And Option System](./configuration-and-option-system.md)
5. [CDC Pipeline Architecture](./cdc-pipeline-architecture.md) if schema is expected to change at runtime
