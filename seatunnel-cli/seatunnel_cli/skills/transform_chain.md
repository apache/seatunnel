<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

---
name: transform_chain
description: Data transformation pipeline using SeaTunnel transform plugins
triggers:
  - transform
  - filter
  - sql
  - replace
  - split
  - copy
  - field
  - mask
  - encrypt
  - embedding
  - llm
  - upper
  - lower
  - convert
  - rename
  - map
  - clean
  - deduplicate
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants to apply data transformations between source and sink — filtering rows,
transforming fields, SQL expressions, field mapping, data masking, LLM enrichment, etc.

## Domain Knowledge

SeaTunnel has these transform plugins:

### SQL-based
- **Sql**: Transform data using SQL expressions. Most flexible — supports WHERE, JOIN-like ops, aggregations, type casting. Use this when the user's transformation can be expressed as SQL.

### Field Operations
- **FieldMapper**: Rename, reorder, or drop fields. Use for schema reshaping.
- **Copy**: Duplicate a field under a new name.
- **Split**: Split a field into multiple fields by separator.
- **Replace**: Regex-based field value replacement.
- **Filter**: Select specific fields to pass through (whitelist).
- **FilterRowKind**: Filter rows by change type (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE) — useful after CDC sources.

### AI-powered
- **LLM**: Call an LLM API to enrich or transform data (classification, extraction, summarization).
- **Embedding**: Generate vector embeddings for text fields.

### Data Quality
- **JsonPath**: Extract values from JSON string fields.
- **DynamicCompile**: Custom Java code transformation (advanced).

### Transform Chaining
Transforms are chained via `plugin_input` / `plugin_output`:
- Source outputs to label A
- Transform 1 reads from A, outputs to B
- Transform 2 reads from B, outputs to C
- Sink reads from C

Each transform block MUST have both `plugin_input` and `plugin_output`.

## SOP

1. **Identify what transformation** the user needs.
2. **Select transform plugin(s)** — prefer Sql for complex logic, specific plugins for simple ops.
3. **Determine chain order** if multiple transforms are needed.
4. **Set routing chain**: source → transform_1 → transform_2 → ... → sink via plugin_output/plugin_input labels.
5. **Fill transform options** using `get_connector_info` for exact option keys.
6. **Generate config** with `transform { }` section between source and sink.
7. **Validate chain**: every plugin_output has a downstream plugin_input consumer.

## Constraints

- Transform blocks go in a `transform { }` section (separate from source/sink).
- Every transform MUST have both `plugin_input` and `plugin_output`.
- The routing chain must be connected: source.plugin_output → transform.plugin_input → transform.plugin_output → sink.plugin_input.
- Do NOT use transforms when a simple source query (SQL) can achieve the same result — avoid unnecessary complexity.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "<mode>"
}

source {
  <SourceConnector> {
    <source_options>
    plugin_output = "<src_label>"
  }
}

transform {
  <TransformPlugin> {
    plugin_input = "<src_label>"
    plugin_output = "<transform_label>"
    <transform_options>
  }
}

sink {
  <SinkConnector> {
    <sink_options>
    plugin_input = "<transform_label>"
  }
}
```
