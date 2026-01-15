---
sidebar_position: 1
---

# Connectors Overview

SeaTunnel provides a comprehensive set of connectors for data integration.

## Quick Stats

| Metric | Count |
|--------|-------|
| Source Connectors | 79 |
| Sink Connectors | 78 |
| CDC Connectors | 7 |
| Supported Engines | SeaTunnel Zeta, Flink, Spark |

## All Connectors

> **Maintenance Guide**: When adding a new connector, simply add a row to the appropriate table below. The tables are sorted alphabetically for easy lookup.

### Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Supported |
| ❌ | Not Supported |
| S | Source |
| K | Sink |

---

## Database Connectors

| Connector | S | K | CDC | Batch | Stream | Exactly-Once | Multi-Table | Doc |
|-----------|---|---|-----|-------|--------|--------------|-------------|-----|
| Clickhouse | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | [Source](source/Clickhouse.md) / [Sink](sink/Clickhouse.md) |
| Cloudberry | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | [Source](source/Cloudberry.md) / [Sink](sink/Cloudberry.md) |
| Databend | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Databend.md) / [Sink](sink/Databend.md) |
| DB2 | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/DB2.md) / [Sink](sink/DB2.md) |
| Doris | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/Doris.md) / [Sink](sink/Doris.md) |
| Druid | ❌ | ✅ | ❌ | - | - | ❌ | ❌ | [Sink](sink/Druid.md) |
| Greenplum | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Greenplum.md) / [Sink](sink/Greenplum.md) |
| Hive | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Hive.md) / [Sink](sink/Hive.md) |
| HiveJdbc | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/HiveJdbc.md) |
| Jdbc | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/Jdbc.md) / [Sink](sink/Jdbc.md) |
| Kingbase | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/Kingbase.md) / [Sink](sink/Kingbase.md) |
| Maxcompute | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Maxcompute.md) / [Sink](sink/Maxcompute.md) |
| MySQL | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/Mysql.md) / [Sink](sink/Mysql.md) |
| OceanBase | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/OceanBase.md) / [Sink](sink/OceanBase.md) |
| Oracle | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/Oracle.md) / [Sink](sink/Oracle.md) |
| Phoenix | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Phoenix.md) / [Sink](sink/Phoenix.md) |
| PostgreSQL | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/PostgreSQL.md) / [Sink](sink/PostgreSql.md) |
| Redshift | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | [Source](source/Redshift.md) / [Sink](sink/Redshift.md) |
| SelectDB-Cloud | ❌ | ✅ | ❌ | - | - | ✅ | ✅ | [Sink](sink/SelectDB-Cloud.md) |
| Snowflake | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | [Source](source/Snowflake.md) / [Sink](sink/Snowflake.md) |
| SqlServer | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | [Source](source/SqlServer.md) / [Sink](sink/SqlServer.md) |
| StarRocks | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | [Source](source/StarRocks.md) / [Sink](sink/StarRocks.md) |
| Vertica | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | [Source](source/Vertica.md) / [Sink](sink/Vertica.md) |

---

## CDC Connectors

> CDC connectors capture real-time data changes. **Note**: CDC connectors are not supported on Spark engine.

| Connector | Exactly-Once | Parallelism | Multi-Table | Schema Evolution | Doc |
|-----------|--------------|-------------|-------------|------------------|-----|
| MongoDB-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/MongoDB-CDC.md) |
| MySQL-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/MySQL-CDC.md) |
| Opengauss-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/Opengauss-CDC.md) |
| Oracle-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/Oracle-CDC.md) |
| PostgreSQL-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/PostgreSQL-CDC.md) |
| SqlServer-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/SqlServer-CDC.md) |
| TiDB-CDC | ✅ | ✅ | ❌ | ❌ | [Source](source/TiDB-CDC.md) |

---

## NoSQL Connectors

| Connector | S | K | Batch | Stream | Exactly-Once | Doc |
|-----------|---|---|-------|--------|--------------|-----|
| Aerospike | ❌ | ✅ | - | - | ❌ | [Sink](sink/Aerospike.md) |
| AmazonDynamoDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/AmazonDynamoDB.md) / [Sink](sink/AmazonDynamoDB.md) |
| Cassandra | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Cassandra.md) / [Sink](sink/Cassandra.md) |
| GoogleFirestore | ❌ | ✅ | - | - | ❌ | [Sink](sink/GoogleFirestore.md) |
| Hbase | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Hbase.md) / [Sink](sink/Hbase.md) |
| HugeGraph | ❌ | ✅ | - | - | ❌ | [Sink](sink/HugeGraph.md) |
| MongoDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/MongoDB.md) / [Sink](sink/MongoDB.md) |
| Neo4j | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Neo4j.md) / [Sink](sink/Neo4j.md) |
| Redis | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Redis.md) / [Sink](sink/Redis.md) |

---

## Data Lake Connectors

| Connector | S | K | Exactly-Once | CDC Support | Multi-Table | Doc |
|-----------|---|---|--------------|-------------|-------------|-----|
| Fluss | ❌ | ✅ | ❌ | ✅ | ✅ | [Sink](sink/Fluss.md) |
| Hudi | ❌ | ✅ | ❌ | ✅ | ❌ | [Sink](sink/Hudi.md) |
| Iceberg | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Iceberg.md) / [Sink](sink/Iceberg.md) |
| Paimon | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/Paimon.md) / [Sink](sink/Paimon.md) |

---

## Message Queue Connectors

| Connector | S | K | Batch | Stream | Exactly-Once | Doc |
|-----------|---|---|-------|--------|--------------|-----|
| Activemq | ❌ | ✅ | - | - | ❌ | [Sink](sink/Activemq.md) |
| AmazonSqs | ✅ | ✅ | ❌ | ✅ | ❌ | [Source](source/AmazonSqs.md) / [Sink](sink/AmazonSqs.md) |
| Kafka | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/Kafka.md) / [Sink](sink/Kafka.md) |
| Pulsar | ✅ | ✅ | ❌ | ✅ | ✅ | [Source](source/Pulsar.md) / [Sink](sink/Pulsar.md) |
| Rabbitmq | ✅ | ✅ | ❌ | ✅ | ❌ | [Source](source/Rabbitmq.md) / [Sink](sink/Rabbitmq.md) |
| RocketMQ | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/RocketMQ.md) / [Sink](sink/RocketMQ.md) |

---

## File System Connectors

> All file connectors support formats: JSON, CSV, Parquet, ORC, Text, XML, Excel, Binary

| Connector | S | K | Exactly-Once | Multi-Table | Multimodal | Doc |
|-----------|---|---|--------------|-------------|------------|-----|
| CosFile | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/CosFile.md) / [Sink](sink/CosFile.md) |
| FtpFile | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/FtpFile.md) / [Sink](sink/FtpFile.md) |
| HdfsFile | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/HdfsFile.md) / [Sink](sink/HdfsFile.md) |
| LocalFile | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/LocalFile.md) / [Sink](sink/LocalFile.md) |
| ObsFile | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/ObsFile.md) / [Sink](sink/ObsFile.md) |
| OssFile | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/OssFile.md) / [Sink](sink/OssFile.md) |
| OssJindoFile | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/OssJindoFile.md) / [Sink](sink/OssJindoFile.md) |
| S3File | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/S3File.md) / [Sink](sink/S3File.md) |
| SftpFile | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/SftpFile.md) / [Sink](sink/SftpFile.md) |

---

## Search Engine Connectors

| Connector | S | K | Batch | Stream | CDC Support | Doc |
|-----------|---|---|-------|--------|-------------|-----|
| Easysearch | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Easysearch.md) / [Sink](sink/Easysearch.md) |
| Elasticsearch | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Elasticsearch.md) / [Sink](sink/Elasticsearch.md) |
| Typesense | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Typesense.md) / [Sink](sink/Typesense.md) |

---

## Time Series Connectors

| Connector | S | K | Batch | Stream | Exactly-Once | Doc |
|-----------|---|---|-------|--------|--------------|-----|
| InfluxDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/InfluxDB.md) / [Sink](sink/InfluxDB.md) |
| IoTDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/IoTDB.md) / [Sink](sink/IoTDB.md) |
| IoTDBv2 | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/IoTDBv2.md) / [Sink](sink/IoTDBv2.md) |
| Prometheus | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Prometheus.md) / [Sink](sink/Prometheus.md) |
| TDengine | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/TDengine.md) / [Sink](sink/TDengine.md) |

---

## Vector Database Connectors

| Connector | S | K | Batch | Stream | Doc |
|-----------|---|---|-------|--------|-----|
| Milvus | ✅ | ✅ | ✅ | ❌ | [Source](source/Milvus.md) / [Sink](sink/Milvus.md) |
| Qdrant | ✅ | ✅ | ✅ | ❌ | [Source](source/Qdrant.md) / [Sink](sink/Qdrant.md) |

---

## API & HTTP Connectors

| Connector | S | K | Batch | Stream | Doc |
|-----------|---|---|-------|--------|-----|
| Github | ✅ | ❌ | ✅ | ❌ | [Source](source/Github.md) |
| Gitlab | ✅ | ❌ | ✅ | ❌ | [Source](source/Gitlab.md) |
| GoogleSheets | ✅ | ❌ | ✅ | ❌ | [Source](source/GoogleSheets.md) |
| GraphQL | ✅ | ✅ | ✅ | ✅ | [Source](source/GraphQL.md) / [Sink](sink/GraphQL.md) |
| Http | ✅ | ✅ | ✅ | ✅ | [Source](source/Http.md) / [Sink](sink/Http.md) |
| Jira | ✅ | ❌ | ✅ | ❌ | [Source](source/Jira.md) |
| Klaviyo | ✅ | ❌ | ✅ | ❌ | [Source](source/Klaviyo.md) |
| Lemlist | ✅ | ❌ | ✅ | ❌ | [Source](source/Lemlist.md) |
| MyHours | ✅ | ❌ | ✅ | ❌ | [Source](source/MyHours.md) |
| Notion | ✅ | ❌ | ✅ | ❌ | [Source](source/Notion.md) |
| OneSignal | ✅ | ❌ | ✅ | ❌ | [Source](source/OneSignal.md) |
| Persistiq | ✅ | ❌ | ✅ | ❌ | [Source](source/Persistiq.md) |
| Socket | ✅ | ✅ | ✅ | ✅ | [Source](source/Socket.md) / [Sink](sink/Socket.md) |
| Web3j | ✅ | ❌ | ✅ | ✅ | [Source](source/Web3j.md) |

---

## Cloud Service Connectors

| Connector | S | K | Description | Doc |
|-----------|---|---|-------------|-----|
| Datahub | ❌ | ✅ | LinkedIn DataHub | [Sink](sink/Datahub.md) |
| S3-Redshift | ❌ | ✅ | S3 staging to Redshift | [Sink](sink/S3-Redshift.md) |
| Sls | ✅ | ✅ | Alibaba Cloud Log Service | [Source](source/Sls.md) / [Sink](sink/Sls.md) |
| Tablestore | ✅ | ✅ | Alibaba Cloud Tablestore | [Source](source/Tablestore.md) / [Sink](sink/Tablestore.md) |

---

## Notification Connectors

| Connector | Description | Doc |
|-----------|-------------|-----|
| DingTalk | DingTalk robot | [Sink](sink/DingTalk.md) |
| Email | Send emails | [Sink](sink/Email.md) |
| Enterprise-WeChat | WeChat Work robot | [Sink](sink/Enterprise-WeChat.md) |
| Feishu | Feishu/Lark robot | [Sink](sink/Feishu.md) |
| Sentry | Sentry events | [Sink](sink/Sentry.md) |
| Slack | Slack messages | [Sink](sink/Slack.md) |

---

## Utility Connectors

| Connector | Type | Description | Doc |
|-----------|------|-------------|-----|
| Assert | Sink | Data validation for testing | [Sink](sink/Assert.md) |
| Console | Sink | Print to console | [Sink](sink/Console.md) |
| FakeSource | Source | Generate test data | [Source](source/FakeSource.md) |

---

## Other Connectors

| Connector | S | K | Description | Doc |
|-----------|---|---|-------------|-----|
| ClickhouseFile | ❌ | ✅ | Write Clickhouse local files | [Sink](sink/ClickhouseFile.md) |
| Kudu | ✅ | ✅ | Apache Kudu | [Source](source/Kudu.md) / [Sink](sink/Kudu.md) |
| OpenMldb | ✅ | ❌ | OpenMLDB feature platform | [Source](source/OpenMldb.md) |
| SensorsData | ❌ | ✅ | Sensors Analytics | [Sink](sink/SensorsData.md) |

---

## Feature Definitions

| Feature | Description |
|---------|-------------|
| **S** | Source connector available |
| **K** | Sink connector available |
| **Batch** | Bounded data processing |
| **Stream** | Unbounded continuous processing |
| **Exactly-Once** | Each record processed exactly once |
| **CDC Support** | Handle INSERT/UPDATE/DELETE operations |
| **Multi-Table** | Process multiple tables in one job |
| **Multimodal** | Support binary/unstructured data |
| **Schema Evolution** | Handle schema changes automatically |

For detailed feature definitions, see [Connector V2 Features](../introduction/concepts/connector-v2-features.md).

## Related Documentation

- [Source Common Options](common-options/source-common-options.md)
- [Sink Common Options](common-options/sink-common-options.md)
- [Data Formats](formats/avro.md)
- [Connector Changelog](changelog/connector-common.md)
