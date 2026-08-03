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
name: multi_pipeline
description: Multiple data pipelines with independent source-sink routing in one job
triggers:
  - multi
  - multiple
  - pipelines
  - pipeline
  - routes
  - routing
  - 2 pipelines
  - 3 pipelines
  - several
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants multiple independent data paths within a single SeaTunnel job.
Examples: "sync table A to Console and table B to Assert", "read from 2 sources
and write to different sinks", any scenario with more than one source-sink pair.

Also activated when the plan contains 2+ pipeline entries.

## Domain Knowledge

- SeaTunnel supports multiple source and sink blocks inside a single `source { }` / `sink { }` section.
- Routing is done via `plugin_output` (on source) and `plugin_input` (on sink). These labels link a specific source to its corresponding sink.
- Each `plugin_output` value MUST be unique across all source blocks.
- Each `plugin_input` value MUST match exactly one `plugin_output` value.
- Multiple blocks of the SAME connector type are allowed (e.g., two Jdbc source blocks reading different tables).
- All source blocks go inside ONE `source { }` section — do NOT create multiple `source { }` sections.
- All sink blocks go inside ONE `sink { }` section — do NOT create multiple `sink { }` sections.
- When a sink connector does NOT support multi-table input (e.g., Console, Assert, Clickhouse), each table needs its own source→sink path. This is called **pipeline expansion**.

### Pipeline Expansion Rule

If user says "sync 3 tables to Console via 1 pipeline", the actual config needs 3 source blocks and 3 sink blocks because Console cannot receive multiple tables through one block.
Expansion is needed when:
- Sink is Console, Assert, Clickhouse, or other single-table sinks
- Source has multiple tables

Expansion is NOT needed when:
- Sink supports multi-table (e.g., Jdbc with `generate_sink_sql = true`)
- Source uses multi-table syntax natively (e.g., CDC with regex `table-name`)

## SOP

1. **Count pipelines** from the structured plan.
2. **Expand pipelines** if needed: for each pipeline with N tables and a single-table sink, expand to N sub-pipelines (one per table).
3. **Generate unique routing labels** for each sub-pipeline: `<connector>_<table>` format.
4. **Fill options** for each source and sink block independently.
5. **Build routing table**: list all (plugin_output → plugin_input) pairs for verification.
6. **Generate config**: ALL source blocks in one `source { }` section, ALL sink blocks in one `sink { }` section.
7. **Validate routing**: every plugin_output has a matching plugin_input, no duplicates, no orphans.

## Constraints

- NEVER create multiple `source { }` or `sink { }` top-level sections.
- Every source block MUST have `plugin_output`.
- Every sink block MUST have `plugin_input`.
- Routing labels MUST be unique — no two sources share the same `plugin_output`.
- When expanding pipelines, each expanded sub-pipeline gets its own routing label.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "<mode>"
}

source {
  <SourceConnector> {
    <source_options_for_table_1>
    plugin_output = "<label_1>"
  }
  <SourceConnector> {
    <source_options_for_table_2>
    plugin_output = "<label_2>"
  }
}

sink {
  <SinkConnector> {
    <sink_options_for_table_1>
    plugin_input = "<label_1>"
  }
  <SinkConnector> {
    <sink_options_for_table_2>
    plugin_input = "<label_2>"
  }
}
```
