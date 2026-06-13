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
name: cdc_realtime
description: Real-time Change Data Capture streaming from CDC source to sink
triggers:
  - cdc
  - CDC
  - real-time
  - realtime
  - real time
  - streaming
  - STREAMING
  - binlog
  - change data capture
  - incremental
  - live sync
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants continuous, real-time data synchronization using CDC connectors
(MySQL-CDC, PostgreSQL-CDC, MongoDB-CDC, Oracle-CDC, SqlServer-CDC, etc.).
Data flows as a never-ending stream of insert/update/delete events.

## Domain Knowledge

- CDC connectors capture binlog/WAL/oplog events and emit them as a continuous stream.
- SeaTunnel CDC mode requires `job.mode = "STREAMING"` in the env block.
- `checkpoint.interval` (milliseconds) controls how often state is checkpointed. Default 10000 (10s) is safe for most cases.
- CDC sources use `database-name` and `table-name` (hyphenated keys, NOT underscored).
- `table-name` supports regex: `"mydb\\.orders"` or `"mydb\\..*"` for all tables.
- CDC sources do NOT use `query` — they capture the full table changelog.
- The sink must support upsert/delete semantics for CDC to work correctly (e.g., StarRocks, Doris, Jdbc with primary keys).
- For multi-table CDC, use `table-name` regex or create multiple CDC source blocks.
- CDC sources need database permissions: REPLICATION SLAVE, REPLICATION CLIENT for MySQL.

## SOP

1. **Identify CDC source connector** (MySQL-CDC, PostgreSQL-CDC, etc.) from the plan.
2. **Identify sink connector** — verify it supports upsert semantics for CDC correctness.
3. **Set env block**: `job.mode = "STREAMING"`, `checkpoint.interval = 10000`.
4. **Fill CDC source options**: hostname, port, username, password, database-name, table-name. Use `get_connector_info` for exact option keys.
5. **Fill sink options**: fetch connector metadata, ensure primary key / upsert config is set.
6. **Set routing**: `plugin_output` on source, `plugin_input` on sink.
7. **Apply golden example** if available for this (source, sink) pair.
8. **Validate**: CDC-specific options present, streaming mode set, checkpoint configured.

## Constraints

- MUST use `job.mode = "STREAMING"` — never BATCH for CDC.
- MUST include `checkpoint.interval` in env block.
- CDC source option keys use hyphens: `database-name`, `table-name` — NOT underscores.
- Do NOT add `query` to CDC sources — they capture changelogs, not arbitrary SQL.
- Credentials always as `${ENV_VAR}`.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  <CDC-Connector> {
    hostname = "<host>"
    port = <port>
    username = "${DB_USER}"
    password = "${DB_PASSWORD}"
    database-name = "<database>"
    table-name = "<table>"
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
