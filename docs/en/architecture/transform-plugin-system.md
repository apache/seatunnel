---
title: Transform Plugin System
---

# Transform Plugin System

## Why This Page Exists

SeaTunnel already has a generated transform catalog and a page for transform common options. What is still missing is a system-level explanation of how transforms fit into the pipeline, what contracts they share, and how contributors should think about them.

This page fills that gap.

## Where Transforms Sit In A Job

Transforms sit between source and sink and operate on SeaTunnel's own row and table model:

```text
Source -> Transform Chain -> Sink
```

In practice, the transform block is optional, but it becomes the main place to express pipeline logic when:

- source fields do not match sink fields directly
- rows must be filtered, enriched, or reshaped
- CDC metadata needs to be converted into a downstream-friendly form
- one job needs to route or reshape multiple logical tables

SeaTunnel uses `plugin_output` to register an intermediate dataset and `plugin_input` to consume one or more previously produced datasets. This lets transforms form a logical graph instead of a single rigid linear chain.

## What Transforms Are Responsible For

At a system level, transforms do more than field-level mapping. They are responsible for:

- reshaping rows without binding the job to an engine-specific record type
- preserving or updating schema information when columns are added, removed, or renamed
- exposing metadata such as row kind or event time as normal fields for downstream logic
- routing, merging, or filtering logical tables in multi-table jobs
- keeping job logic declarative so the same pipeline can run on different engines

This is why the transform layer matters in both batch pipelines and CDC pipelines.

## Core Contracts

The transform system is built around a small set of contracts:

- `SeaTunnelTransform`: the base runtime contract
- `SeaTunnelMapTransform`: one-input to one-output row transformation
- `SeaTunnelFlatMapTransform`: one-input to zero-or-more output rows
- `TableTransform`: wrapper that creates a runtime transform instance
- `TableTransformFactory`: SPI entry point used for discovery and creation
- `TableTransformFactoryContext`: factory context carrying `ReadonlyConfig`, class loader, and upstream `CatalogTable` metadata

This contract split matters because SeaTunnel wants transform plugins to stay:

- declarative from the user's point of view
- engine-independent from the contributor's point of view
- metadata-aware from the planner's point of view

Related docs:

- [Core API Design](./core-api-design.md)
- [Configuration And Option System](./configuration-and-option-system.md)
- [Plugin Discovery and Class Loading](./plugin-discovery-and-class-loading.md)

## How A Transform Is Prepared And Executed

At a high level, transform preparation works like this:

1. the job config defines a transform block and its options
2. SeaTunnel discovers the matching `TableTransformFactory` through the factory and SPI mechanism
3. options are validated before the runtime transform is created
4. upstream `CatalogTable` metadata is passed into the transform factory context
5. the runtime transform is inserted into the logical pipeline and later adapted to the chosen engine

The key design choice is that the transform plugin works on SeaTunnel contracts first. Translation to Flink, Spark, or native Zeta execution happens later.

## Common Transform Categories

The current transform ecosystem is broad, but most plugins fall into a few categories:

### Row Projection And Mapping

- [FieldMapper](../transforms/field-mapper.md)
- [FieldRename](../transforms/field-rename.md)
- [Copy](../transforms/copy.md)

These plugins are used when the main task is to align source fields with downstream schema expectations.

### Filtering And Routing

- [Filter](../transforms/filter.md)
- [TableFilter](../transforms/table-filter.md)
- [TableMerge](../transforms/table-merge.md)

These plugins decide which records or tables continue through the pipeline.

### SQL And Expression-Oriented Processing

- [SQL](../transforms/sql.md)
- [JsonPath](../transforms/jsonpath.md)
- [RegexExtract](../transforms/regexextract.md)

These plugins are useful when the transformation logic is easier to express declaratively than with custom code.

### Metadata And CDC Adaptation

- [Metadata](../transforms/metadata.md)
- [RowKindExtractor](../transforms/rowkind-extractor.md)
- [FilterRowKind](../transforms/filter-rowkind.md)

These plugins are especially important in CDC pipelines because they help preserve or reshape change semantics for downstream systems.

### Programmable Or AI-Oriented Processing

- [DynamicCompile](../transforms/dynamic-compile.md)
- [LLM](../transforms/llm.md)
- [Embedding](../transforms/embedding.md)

These plugins are used when row processing needs external models, richer computation, or custom business logic.

## Design Guidelines For Contributors

When adding or reviewing a transform plugin, check these points first:

- keep the transform contract engine-independent
- define options through stable `Option` and `OptionRule` contracts
- make schema changes explicit instead of leaving downstream ambiguity
- handle multi-table inputs and outputs deliberately when the plugin can be used in that mode
- avoid leaking source-specific or sink-specific responsibilities into the transform layer

In general, transforms should own row and schema shaping logic, not external commit semantics or engine runtime behavior.

## Common Misunderstandings

### "Transforms are only optional decoration"

Not really. In many jobs the transform layer is where the actual business mapping, schema alignment, and CDC adaptation happens.

### "Transform logic is always row-only"

Also not true. Many transforms need to preserve or reshape schema and metadata, especially in multi-table and change-event scenarios.

### "If a transform works on one engine, portability is automatic"

Portability is a design goal, not a free side effect. Contributors still need to avoid engine-specific assumptions and follow SeaTunnel's API contracts.

## Recommended Reading Path

1. this page for the system view
2. [Transform Common Options](../transforms/common-options/common-options.md)
3. [Core API Design](./core-api-design.md)
4. [CDC Pipeline Architecture](./cdc-pipeline-architecture.md)
5. [Plugin Discovery and Class Loading](./plugin-discovery-and-class-loading.md)
6. [Transforms Catalog](../transforms)
