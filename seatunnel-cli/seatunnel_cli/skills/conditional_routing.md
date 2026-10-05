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
name: conditional_routing
description: Split one source stream into multiple sinks by row-level conditions (predicate routing)
triggers:
  - split
  - route
  - routing
  - partition by
  - separate
  - filter into
  - based on
  - by condition
  - by status
  - by level
  - greater than
  - less than
  - 拆分
  - 分流
  - 路由
  - 大于
  - 小于
tools:
  - get_connector_info
composable: true
---

## When to Use

User wants rows from ONE source delivered to DIFFERENT sinks depending on a
row-level condition. Examples: "route amount >= 1000 to files, the rest to
console", "ERROR logs to console, everything else to archive", "PAID orders
here, unpaid there".

This is NOT multi-pipeline (independent sources) and NOT fan-out (same rows
to all sinks) — it is one stream split by mutually exclusive predicates.

## Domain Knowledge

- The split is built from **parallel SQL transforms consuming the SAME source
  output**: each transform declares `plugin_input = "<source_label>"` and its
  own WHERE predicate, then emits a distinct `plugin_output`.
- SeaTunnel duplicates the source stream to every transform that references
  its label — no extra config needed on the source.
- Each branch then needs its own sink consuming that branch's label.
- The Zeta SQL transform supports projection + WHERE only (no GROUP BY,
  JOIN, or ORDER BY). Keep predicates simple comparisons/boolean logic.
- In the SQL transform, `table_name` must reference the INPUT label (the
  source's plugin_output), and the FROM clause uses that same name.

## SOP

1. **Identify the split condition** and the number of branches (usually 2).
2. **Make predicates mutually exclusive and complete**: `amount > 100` pairs
   with `amount <= 100` (not `amount < 100`, which silently drops equality).
   If the user's wording leaves a gap, close it and note the choice.
3. **One source block** with a single `plugin_output` label.
4. **One SQL transform per branch**, all with `plugin_input` = source label,
   each with a distinct `plugin_output` and its WHERE predicate.
5. **One sink per branch**, each consuming its branch label.
6. **Validate wiring**: every label consumed exists; branch labels are
   unique; no branch left without a sink.

## Constraints

- Do NOT read the source twice — one source block, shared by transforms.
- Do NOT chain the transforms (both consume the SOURCE label, not each other).
- Predicates MUST be mutually exclusive and SHOULD cover all rows.
- Only WHERE-level SQL (Zeta SQL transform limitation).

## Pattern

Split `transactions` by amount into two destinations:

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://<host>:3306/<db>"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "${MYSQL_USER}"
    password = "${MYSQL_PASSWORD}"
    query = "SELECT * FROM transactions"
    plugin_output = "tx_all"
  }
}

transform {
  Sql {
    plugin_input = "tx_all"
    plugin_output = "tx_large"
    query = "SELECT * FROM tx_all WHERE amount >= 1000"
  }
  Sql {
    plugin_input = "tx_all"
    plugin_output = "tx_small"
    query = "SELECT * FROM tx_all WHERE amount < 1000"
  }
}

sink {
  LocalFile {
    plugin_input = "tx_large"
    path = "/data/tx_large"
    file_format_type = "json"
  }
  Console {
    plugin_input = "tx_small"
  }
}
```

Wiring summary: `source(tx_all) ⇒ [Sql→tx_large, Sql→tx_small] ⇒ sinks`.
Both transforms consume `tx_all`; the two predicates are exclusive
(`>= 1000` / `< 1000`) and together cover every row.
