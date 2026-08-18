---
sidebar_position: 10
---

# Connector FAQ

This page is an index of frequently asked questions organized by connector category. Each entry links to the FAQ section inside the corresponding connector documentation page.

These FAQ sections are intended to be quick navigation, not a second source of truth. For exact
option names, defaults, and full examples, always use the connector option table and linked
detailed sections on the connector page itself.

For general SeaTunnel questions (engine setup, variable substitution, scheduling, etc.) see the [General FAQ](../faq.md).

---

## CDC Connectors

Change Data Capture connectors read real-time change events (INSERT / UPDATE / DELETE) from database transaction logs.

| Connector | Common FAQ Topics |
|---|---|
| [MySQL CDC](./source/MySQL-CDC.md#faq) | Required permissions, binlog settings, replica support, tables without primary keys, snapshot phase, DDL propagation, `server-id` conflicts, snapshot performance, timezone/charset |
| [PostgreSQL CDC](./source/PostgreSQL-CDC.md#faq) | Required permissions, logical decoding plugins, replica support, tables without primary keys, replication slot management, replication lag |
| [Oracle CDC](./source/Oracle-CDC.md#faq) | LogMiner permissions, supplemental logging, CDB/PDB multi-tenant, tables without primary keys, LogMiner performance, supported Oracle versions |

---

## Message Queue Connectors

| Connector | Common FAQ Topics |
|---|---|
| [Kafka Source](./source/Kafka.md#faq) | `start_mode` options, filtering by message key, supported formats, SASL/Kerberos authentication, consumer group offset commit |
| [Kafka Sink](./sink/Kafka.md#faq) | Automatic topic creation, `partition_key_fields` behavior, exactly-once delivery, SASL/Kerberos authentication, supported formats |

---

## Sink Connectors

### OLAP / Analytical Stores

| Connector | Common FAQ Topics |
|---|---|
| [Doris Sink](./sink/Doris.md#faq) | Automatic table creation, exactly-once with 2PC, "Label already exists" error, DELETE propagation, column case sensitivity, Stream Load format |
| [StarRocks Sink](./sink/StarRocks.md#faq) | Automatic table creation, upsert and DELETE support, `labelPrefix` usage, column case sensitivity, `nodeUrls` vs `base-url` |
| [ClickHouse Sink](./sink/Clickhouse.md#faq) | Automatic table creation, batch write performance, supported data types, "Table doesn't exist" error |

### Relational Databases

| Connector | Common FAQ Topics |
|---|---|
| [JDBC Sink](./sink/Jdbc.md#faq) | Automatic table creation, exactly-once with XA transactions, upsert / primary key configuration, multi-table writing, missing JDBC driver |

### Data Lakes / File Systems

| Connector | Common FAQ Topics |
|---|---|
| [Hive Sink](./sink/Hive.md#faq) | Supported file formats, partitioned tables, Kerberos authentication, small file problem, schema evolution |

---

## Tips for Finding Answers

1. **Connector-specific issues** → go directly to the connector's page and scroll to its **FAQ** section.
2. **Type and schema questions** (e.g., JSON schema, Debezium schema, Oracle CLOB, MySQL TINYINT, ORC/Parquet mapping) → see [Type and Schema FAQ](./type-schema-faq.md).
3. **File and object storage questions** (e.g., MinIO/S3, small files, save modes, multi-table file paths) → see [File and Object Storage FAQ](./file-object-storage-faq.md).
4. **Cross-connector topics** (e.g., "does SeaTunnel support CDC?", "what is `schema_save_mode`?") → see the [General FAQ](../faq.md).
5. **Still stuck?** → search the [GitHub Issues](https://github.com/apache/seatunnel/issues) or reach out via the [mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org).
2. **Cross-connector sink write topics** (e.g., "`generate_sink_sql` vs `query`", "`schema_save_mode`", "`data_save_mode`", or `enable_upsert`) → see [Sink Write Modes and Save Modes](./common-options/sink-write-modes.md).
3. **Other cross-connector topics** (e.g., "does SeaTunnel support CDC?") → see the [General FAQ](../faq.md).
4. **Still stuck?** → search the [GitHub Issues](https://github.com/apache/seatunnel/issues) or reach out via the [mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org).
