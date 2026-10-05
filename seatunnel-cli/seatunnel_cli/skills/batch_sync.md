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
name: batch_sync
description: Batch data synchronization from source to sink (one-time or scheduled full/incremental load)
triggers:
  - batch
  - sync
  - migrate
  - import
  - export
  - dump
  - load
  - copy
  - transfer
  - extract
  - BATCH
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants to move data from one system to another in batch mode — full table sync,
SELECT-based extraction, one-time migration, scheduled ETL load.
This is the **default skill** when no streaming/CDC/real-time keywords are present.

## Domain Knowledge

- SeaTunnel batch mode uses `job.mode = "BATCH"` in the `env` block.
- Parallelism controls how many threads read/write concurrently. Default 2 is safe; increase for large tables.
- JDBC sources use `query` for flexible extraction; `table_path` for auto-split parallel reads.
- For multi-table batch sync, each table needs its own source block with a unique `plugin_output` routing label.
- Credentials MUST use `${ENV_VAR}` placeholders — never hardcode passwords.
- `plugin_output` on source and `plugin_input` on sink link data paths together.
- When a single source reads multiple tables via separate queries, generate one source block per table.

## SOP

1. **Identify source and sink connectors** from the plan.
2. **Determine tables** — if user specifies tables, use them; if not, flag as missing.
3. **Set env block**: `job.mode = "BATCH"`, `parallelism = <N>`.
4. **Fill source options**: fetch connector metadata via `get_connector_info`, fill required options.
5. **Fill sink options**: fetch connector metadata, fill required options. Use `generate_sink_sql = true` for Jdbc sink when schema auto-creation is desired.
6. **Set routing**: each source gets `plugin_output = "<label>"`, matching sink gets `plugin_input = "<label>"`.
7. **Apply golden example** if available for this (source, sink) pair — use as structural reference.
8. **Validate**: all required options present, routing labels matched, no duplicate plugin_output values.

## Constraints

- NEVER set `job.mode = "STREAMING"` — that belongs to cdc_realtime skill.
- NEVER add `checkpoint.interval` — not needed for batch.
- Credentials always as `${ENV_VAR}`, never plaintext.
- Do NOT invent option keys — only use keys from connector metadata.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "BATCH"
}

source {
  <SourceConnector> {
    <source_options>
    plugin_output = "<routing_label>"
  }
}

sink {
  <SinkConnector> {
    <sink_options>
    plugin_input = "<routing_label>"
  }
}
```
