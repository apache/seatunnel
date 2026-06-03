---
sidebar_position: 16
---

# Multi-Table Transform Capability Boundary

## Overview

SeaTunnel's **multi-table transform** feature allows a single transform node to process multiple
tables flowing from an upstream source (typically a CDC connector) in one pipeline. This page
documents precisely what is supported, what is not, and what alternatives to use when you hit a
capability boundary.

---

## 1. What Is a Multi-Table Transform?

In a standard single-table pipeline, one Source feeds one Transform chain feeds one Sink.
In a multi-table pipeline, a single Source (e.g., MySQL-CDC) emits records from **many tables**
simultaneously, and each downstream Transform or Sink must declare which table(s) it applies to.

```
MySQL-CDC ──► FieldMapper (orders table)  ──► Kafka Sink (orders topic)
           │
           ├──► FieldMapper (users table)  ──► Kafka Sink (users topic)
           │
           └──► (unmatched tables pass through) ──► Elasticsearch Sink
```

---

## 2. Capability Boundary Table

| Capability | Supported | Notes |
|---|---|---|
| Per-table field rename / map | ✅ Yes | Use `FieldMapper` with `plugin_input` and `table_match_regex` |
| Per-table column filtering | ✅ Yes | Use `Filter` with `plugin_input` and `table_match_regex` |
| Per-table type casting | ✅ Yes | Use `FieldMapper` with `define_sink_type` option |
| Per-table SQL transform (single table) | ✅ Yes | Use `SQL` transform with `plugin_input`; scope it with `table_match_regex` when needed |
| `TableMerge` — merge multiple tables into one | ✅ Yes | Tables must share compatible schema |
| `TableRename` — rename tables in the stream | ✅ Yes | Works well for routing to Sink by table name |
| Row-level filtering (filter by `rowkind`) | ✅ Yes | Use `FilterRowKind` transform |
| Cross-table SQL JOIN | ❌ Not supported | See Section 4 for alternatives |
| Aggregation across multiple CDC tables | ❌ Not supported | Aggregate downstream in OLAP engine |
| Generating new tables from a JOIN result | ❌ Not supported | Use a dedicated SQL engine |
| Applying one transform to ALL tables wildcard | ⚠️ Partial | `TableMerge` then single transform; schema must be compatible |
| Changing schema mid-stream (DDL events) | ⚠️ Limited | Depends on sink; some sinks handle schema evolution; transforms do not |
| Nested JSON field extraction per table | ✅ Yes | Use `JsonPath` transform with `plugin_input` and `table_match_regex` |

---

## 3. TableMerge vs SQL Join

These two are the most commonly confused features.

### 3.1 `TableMerge` Transform

`TableMerge` **merges the row streams of multiple tables into a single result table**. All
input tables must have the same (or compatible) schema. Use it to route all tables to one Sink.

```json
{
  "plugin_name": "TableMerge",
  "plugin_input": ["orders_2023", "orders_2024"],
  "plugin_output": "all_orders",
  "merge_by_field": true
}
```

**When to use**: Fan-in from multiple source tables that have the same structure (e.g., sharded
tables, multi-year partitions, or multi-database tables with identical schema).

**When NOT to use**: When tables have different schemas that you need to correlate or enrich
from each other — that requires a JOIN.

### 3.2 SQL JOIN (not natively supported in multi-table pipelines)

A SQL JOIN correlates rows from two different tables based on a key. **SeaTunnel's `SQL`
transform does NOT support cross-table JOIN inside a multi-table streaming pipeline.**

Attempting to JOIN records from two upstream tables inside a single SQL transform is not
supported and will result in a configuration error.

**Recommended alternatives**:
- Write both tables to a shared data lake or warehouse (e.g., Hudi, Iceberg, ClickHouse), then
  run the JOIN there
- Use Apache Flink with SeaTunnel's Flink connector for stateful JOIN operations
- Materialise the "dimension" table into a lookup cache (e.g., Redis, RocksDB) and use a custom
  transform for enrichment

---

## 4. Cross-Source JOIN Limitation

SeaTunnel does **not** support streaming JOINs where the two input sides come from **different
sources** (e.g., joining MySQL-CDC with a PostgreSQL-CDC stream).

| Scenario | Supported |
|---|---|
| Single-source multi-table pass-through | ✅ |
| Single-source TableMerge (same schema) | ✅ |
| Cross-source JOIN (MySQL-CDC + PG-CDC) | ❌ |
| Cross-source JOIN (CDC + JDBC batch) | ❌ |
| Same-source JOIN on two different tables | ❌ |

**Workaround**: Write both sources to a common sink (Kafka, Iceberg, etc.) and perform the JOIN
downstream in a dedicated SQL engine (Flink, Spark, ClickHouse, etc.).

---

## 5. Per-Table Transform Configuration Pattern

When you need different transforms for different tables from the same source, declare separate
transform blocks that share the same `plugin_input` and use different `table_match_regex`
rules:

```json
{
  "env": {
    "job.name": "cdc-multi-table",
    "job.mode": "STREAMING"
  },
  "source": [
    {
      "plugin_name": "MySQL-CDC",
      "plugin_output": "cdc_stream",
      "base-url": "jdbc:mysql://localhost:3306/mydb",
      "username": "cdc_user",
      "password": "password",
      "database-names": ["mydb"],
      "table-names": ["mydb.orders", "mydb.users", "mydb.products"]
    }
  ],
  "transform": [
    {
      "plugin_name": "FieldMapper",
      "plugin_input": ["cdc_stream"],
      "plugin_output": "orders_mapped",
      "field_mapper": { "order_id": "id", "order_amount": "amount" },
      "table_match_regex": "mydb\\.orders"
    },
    {
      "plugin_name": "FieldMapper",
      "plugin_input": ["cdc_stream"],
      "plugin_output": "users_mapped",
      "field_mapper": { "user_id": "id", "user_email": "email" },
      "table_match_regex": "mydb\\.users"
    }
  ],
  "sink": [
    {
      "plugin_name": "Kafka",
      "plugin_input": ["orders_mapped"],
      "topic": "orders"
    },
    {
      "plugin_name": "Kafka",
      "plugin_input": ["users_mapped"],
      "topic": "users"
    },
    {
      "plugin_name": "Kafka",
      "plugin_input": ["cdc_stream"],
      "topic": "products",
      "table_match_regex": "mydb\\.products"
    }
  ]
}
```

---

## 6. Common Fields Example (Shared Schema)

If multiple tables share a common set of fields, you can use `TableMerge` to combine them and
apply a single transform:

```json
"transform": [
  {
    "plugin_name": "TableMerge",
    "plugin_input": ["cdc_stream"],
    "plugin_output": "all_events",
    "table_match_regex": "mydb\\.(orders|payments|refunds)"
  },
  {
    "plugin_name": "FieldMapper",
    "plugin_input": ["all_events"],
    "plugin_output": "all_events_mapped",
    "field_mapper": { "created_at": "event_time", "event_type": "type" }
  }
]
```

This works only when all three tables (`orders`, `payments`, `refunds`) share `created_at` and
`event_type` fields. If schemas differ, `TableMerge` will fail at runtime.

---

## 7. EtLT Patterns with Multi-Table Transform

**EtLT** (Extract, light-transform, Load, then Transform in the warehouse) is the recommended
pattern when SeaTunnel's transform layer cannot fulfil the full transformation requirement:

```
CDC Source
   │
   ▼
Light transforms (field rename, type cast, row filter)
   │
   ▼
Data Lake / Warehouse (Hudi / Iceberg / ClickHouse)
   │
   ▼
Heavy transforms (JOINs, aggregations, complex SQL)
in dbt / Flink SQL / Spark SQL
```

Use SeaTunnel's transform layer for:
- Field rename / filtering
- Type normalisation
- Row-level filtering
- Schema routing (different tables → different topics/tables)

Offload to downstream:
- Cross-table JOINs
- Aggregations
- Pivot / unpivot
- Enrichment from dimension tables

---

## 8. FAQ

**Q: Can I apply one transform to ALL tables without specifying each one?**

Not directly. Use `TableMerge` to combine tables with compatible schemas first, then apply a
single transform to the merged result. If schemas differ, you must use separate transform blocks,
typically with different `table_match_regex` rules.

**Q: Does the `SQL` transform support `JOIN`?**

No. The `SQL` transform only supports single-table queries (SELECT, WHERE, expressions). For
JOINs, use an external SQL engine after loading the data into a sink.

**Q: What happens to tables that are not matched by any transform?**

Unmatched tables continue flowing through the pipeline and can be captured by a Sink that
uses the right `plugin_input` and `table_match_regex`.

**Q: Can I add a new table to an existing CDC pipeline without downtime?**

This depends on the connector. MySQL-CDC supports dynamic table discovery in some configurations,
but adding a transform for a new table requires a pipeline restart. Use `stop-with-savepoint`
to minimise data loss (see [REST API v2 Reference](../engines/zeta/rest-api-v2.md)).

---

## See Also

- [TableMerge Transform Reference](table-merge.md)
- [TableRename Transform Reference](table-rename.md)
- [transform-multi-table Reference](transform-multi-table.md)
- [Multi-Table Architecture Overview](../architecture/features/multi-table.md)
- [CDC Pipeline Architecture](../architecture/cdc-pipeline-architecture.md)
- [REST API v2 Reference](../engines/zeta/rest-api-v2.md)
