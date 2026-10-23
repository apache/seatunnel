---
title: Data Format Handling
---

# Data Format Handling

## Why This Page Exists

SeaTunnel already documents many concrete formats such as Avro, Protobuf, Debezium JSON, Canal JSON, and Maxwell JSON. What is still missing is the system-level explanation of how formats fit into SeaTunnel's pipeline model.

This page explains that role.

## The Core Idea

SeaTunnel separates three concerns that are easy to mix up:

- external storage or transport system
- connector runtime
- record serialization format

For example, Kafka is the transport system, Kafka connector is the runtime bridge, and Avro or Debezium JSON is the format.

This separation matters because the same connector may support multiple formats, and the same format may appear in different connectors.

## Format Position in the Pipeline

A format usually sits at the boundary between SeaTunnel's internal row model and an external byte or message representation.

Typical path:

```text
External bytes / messages
        |
        v
Connector reader
        |
        v
Format decoder
        |
        v
SeaTunnelRow + schema metadata
        |
        v
Transforms and engine runtime
        |
        v
SeaTunnelRow
        |
        v
Format encoder
        |
        v
External bytes / messages
```

This is why format handling is not an afterthought. It directly affects schema, type conversion, CDC semantics, and compatibility with external ecosystems.

## What the Internal Model Looks Like

Inside SeaTunnel, data is generally represented as:

- `SeaTunnelRow`
- `CatalogTable`
- `TableSchema`
- `SeaTunnelDataType`

Formats are responsible for bridging between this internal model and external representations such as:

- JSON documents
- Avro records
- Protobuf messages
- CDC envelope formats

Related docs:

- [Core API Design](./core-api-design.md)
- [CatalogTable and Metadata Management](./api-design/catalog-table.md)

## Common Format Categories

### Plain Row Formats

These formats mainly encode field values and structure:

- JSON
- Avro
- Protobuf
- text and CSV-style formats where supported through file connectors

Typical concerns:

- field order and naming
- nullability
- numeric and temporal type mapping
- schema presence or schema-less mode

### CDC Envelope Formats

These formats carry change semantics in addition to row values:

- Debezium JSON
- CDC-compatible Debezium JSON
- Canal JSON
- Maxwell JSON
- OGG JSON

Typical concerns:

- row kind mapping
- before / after images
- source metadata
- schema change compatibility

Related docs:

- [CDC Pipeline Architecture](./cdc-pipeline-architecture.md)

## Where Format Handling Usually Happens

### Source Side

On the source side, format handling typically decodes external payloads into SeaTunnel's internal row model.

Examples:

- reading Avro or JSON from Kafka
- reading JSON or text-based records from file connectors
- decoding CDC envelopes from message systems

### Sink Side

On the sink side, format handling usually serializes rows into the target system's expected payload shape.

Examples:

- writing Debezium-compatible messages to Kafka
- writing Avro or Protobuf to messaging systems
- writing text or JSON files

## Why Format Handling Is Not Just Serialization

In SeaTunnel, format handling often also decides:

- how schema is inferred or validated
- how external field types map into `SeaTunnelDataType`
- whether CDC metadata is preserved
- whether the target ecosystem can consume the output without custom adapters

That means format design is tightly connected to both type system design and connector usability.

## Schema and Format

Formats interact with schema in several different ways.

### Schema-Driven Formats

Some formats work best when schema is explicit, either from the connector, an external registry, or a user-defined schema block.

Typical examples:

- Avro
- Protobuf
- strongly typed JSON pipelines

### Schema-Lite or Schema-Less Formats

Some formats can be consumed with weaker schema assumptions, but that shifts more responsibility to connector configuration or runtime inference.

Typical examples:

- plain JSON
- text-based payloads

Related docs:

- [Schema Feature](../introduction/concepts/schema-feature.md)

## CDC Format Handling

CDC formats are especially important because they are often used to integrate SeaTunnel with existing ecosystems.

Common reasons to use a CDC envelope format:

- keep compatibility with Debezium consumers
- publish changelog events to Kafka
- preserve before / after semantics for downstream consumers
- bridge database CDC into messaging systems

When documenting or reviewing a CDC format flow, check:

- whether row kind is preserved correctly
- whether source metadata is preserved
- whether topic, table, and key conventions remain compatible downstream

## Type Mapping Risk Areas

The most error-prone areas in format handling are usually:

- decimal precision and scale
- timestamp and time zone semantics
- nested row, map, and array types
- binary encoding
- null handling in schema-less JSON-style payloads

These issues often look like connector bugs, but the real boundary problem is in format conversion.

## Operational Concerns

When a pipeline fails around formats, the symptoms often show up as:

- decode failures
- incompatible schema errors
- downstream consumers rejecting messages
- silent precision loss or unexpected type coercion

The first questions to ask are:

1. Is the connector using the expected format?
2. Does the source and sink agree on schema expectations?
3. Is this a plain row format or a CDC envelope format?
4. Are nested and temporal fields mapped consistently?

## Recommended Reading Path

1. this page for the system view
2. [CatalogTable and Metadata Management](./api-design/catalog-table.md)
3. [Schema Feature](../introduction/concepts/schema-feature.md)
4. a concrete format page under `connectors/formats/`
5. [CDC Pipeline Architecture](./cdc-pipeline-architecture.md) if the format is changelog-oriented
