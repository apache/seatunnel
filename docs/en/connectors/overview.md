---
title: Connectors Overview
---

# SeaTunnel Connectors Capability Overview

SeaTunnel provides a comprehensive set of connectors that enable you to read from various data sources and write to different data sinks. This document provides a detailed capability matrix for all available connectors based on the [Connector V2 Features](../introduction/concepts/connector-v2-features.md).

## Quick Facts

- **Total Source Connectors**: 79
- **Total Sink Connectors**: 78
- **Total Connectors**: 157
- **Supported Engines**: Spark, Flink, SeaTunnel Zeta
- **Supported Data Types**: Structured, Unstructured, Multimodal

## Feature Definitions

### Source Connector Features

| Feature | Description |
|---------|-------------|
| **exactly-once** | Each piece of data is sent downstream only once, with state snapshots and offsets for reliability |
| **column projection** | Read only specified columns from data source efficiently |
| **batch** | Supports bounded data processing (job stops after completing all data) |
| **stream** | Supports unbounded data processing (continuous streaming) |
| **parallelism** | Supports parallel execution with multiple tasks reading different splits |
| **multimodal** | Supports structured and unstructured data (text, video, images, binary files) |
| **support user-defined split** | Users can configure custom split rules |
| **support multiple table read** | Read multiple tables in one SeaTunnel job |

### Sink Connector Features

| Feature | Description |
|---------|-------------|
| **exactly-once** | Each piece of data is written to target only once via key deduplication or XA transactions |
| **cdc** | Supports change data capture with INSERT/UPDATE/DELETE operations based on primary key |
| **support multiple table write** | Write to multiple tables in one SeaTunnel job with dynamic table identifiers |
| **multimodal** | Supports structured and unstructured data (text, video, images, binary files) |

## Source Connectors Capability Matrix

### Database & CDC Connectors

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| **Jdbc** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| MySQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Oracle | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| SQLServer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| DB2 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Kingbase | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Hive | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| HiveJdbc | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Clickhouse | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Doris | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| StarRocks | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Phoenix | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Greenplum | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Redshift | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Vertica | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| **MySQL-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **PostgreSQL-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Oracle-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **SQLServer-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **TiDB-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **MongoDB-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Opengauss-CDC** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### NoSQL Databases

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| MongoDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Cassandra | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Hbase | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Redis | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Neo4j | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Data Lake & Warehouse

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Iceberg | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Hudi | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Paimon | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Databend | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Maxcompute | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| OceanBase | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |

### Message Queues

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Kafka | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Pulsar | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Rabbitmq | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| RocketMQ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| AmazonSqs | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |

### File Systems

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| LocalFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| HdfsFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| S3File | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OssFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OssJindoFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ObsFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| CosFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| FtpFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SftpFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### Time Series & Search Engines

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| InfluxDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| IoTDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| IoTDBv2 | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| TDengine | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Elasticsearch | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Easysearch | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Typesense | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Prometheus | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Vector Databases

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Milvus | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Qdrant | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### APIs & Cloud Services

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Http | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Socket | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Github | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Gitlab | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Jira | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Notion | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| GoogleSheets | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| GraphQL | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| AmazonDynamoDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Klaviyo | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Lemlist | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| MyHours | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| OneSignal | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Persistiq | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Web3j | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Special & Test Connectors

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| FakeSource | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Cloudberry | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Kudu | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| OpenMldb | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Tablestore | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

## Sink Connectors Capability Matrix

### Database Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Jdbc** | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| **MySQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **PostgreSQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Oracle** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **SQLServer** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **DB2** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Kingbase** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Hive** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Phoenix** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Greenplum** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ |

### NoSQL & Graph Databases

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **MongoDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Cassandra** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Hbase** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Redis** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Neo4j** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Aerospike** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **AmazonDynamoDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **GoogleFirestore** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **HugeGraph** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Data Warehouse & Analytical Databases

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Clickhouse** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Doris** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **StarRocks** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Redshift** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Snowflake** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Databend** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Vertica** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Druid** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Message Queue Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Kafka** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Pulsar** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Rabbitmq** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **RocketMQ** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **AmazonSqs** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Activemq** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### File System Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **LocalFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **HdfsFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **S3File** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **OssFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **OssJindoFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **ObsFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **CosFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **FtpFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **SftpFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **ClickhouseFile** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Data Lake Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Iceberg** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Hudi** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Paimon** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Search & Time Series Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Elasticsearch** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Easysearch** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Typesense** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **InfluxDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **IoTDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **IoTDBv2** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **TDengine** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Prometheus** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Vector Database Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Milvus** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Qdrant** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### API & Cloud Service Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Http** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **GraphQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Socket** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Datahub** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Maxcompute** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### Specialized Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Console** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Assert** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Email** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Slack** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **DingTalk** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Feishu** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Enterprise-WeChat** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Sentry** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **SensorsData** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |

### Other Sinks

| Connector | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **S3-Redshift** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **SelectDB-Cloud** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Tablestore** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Kudu** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Cloudberry** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Fluss** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **OceanBase** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

## Feature Support Summary

### Source Connectors Feature Distribution

| Feature | Count | Percentage | Examples |
|--------|-------|------------|----------|
| **Engine Support** | | | |
| All Three Engines | ~65 | 82% | Most database, file, and message queue connectors |
| Spark + Flink Only | ~8 | 10% | Some specialized API connectors |
| Flink + Zeta Only | ~4 | 5% | CDC connectors |
| **Processing Mode** | | | |
| Batch Support | ~60 | 76% | Most database and file connectors |
| Stream Support | ~70 | 89% | Most connectors, especially CDC and messaging |
| **Reliability** | | | |
| Exactly-Once | ~45 | 57% | File connectors, JDBC, Kafka |
| **Performance** | | | |
| Parallelism | ~55 | 70% | Most database and file connectors |
| Column Projection | ~25 | 32% | JDBC, File, some specialized connectors |
| **Advanced Features** | | | |
| User-Defined Split | ~15 | 19% | CDC, some file connectors |
| Multiple Table Read | ~25 | 32% | JDBC and some database connectors |
| Multimodal Support | ~10 | 13% | File and some specialized connectors |

### Sink Connectors Feature Distribution

| Feature | Count | Percentage | Examples |
|--------|-------|------------|----------|
| **Engine Support** | | | |
| All Three Engines | ~70 | 90% | Most database and file connectors |
| Spark + Flink Only | ~6 | 8% | Some specialized connectors |
| Flink + Zeta Only | ~2 | 3% | Specialized cases |
| **Reliability** | | | |
| Exactly-Once | ~40 | 51% | JDBC, Kafka, File connectors |
| **Data Capabilities** | | | |
| CDC Support | ~5 | 6% | Limited to specialized database sinks |
| Multiple Table Write | ~15 | 19% | JDBC and some database sinks |
| Multimodal Support | ~10 | 13% | File and specialized connectors |

## Connector Selection Guide

### Use Case-Based Recommendations

#### For High Throughput Batch Processing
- **Recommended Sources**: Jdbc, File connectors (LocalFile, HdfsFile, S3File)
- **Recommended Sinks**: Jdbc, File connectors, Data Lake formats (Iceberg, Hudi)
- **Key Features**: Batch support, parallelism, column projection

#### For Real-time Stream Processing
- **Recommended Sources**: CDC connectors, Kafka, File connectors (stream mode)
- **Recommended Sinks**: Kafka, Jdbc (with transactions), Real-time databases
- **Key Features**: Stream support, exactly-once, low latency

#### For Exactly-Once Guarantees
- **Recommended Sources**: File connectors, JDBC, Kafka
- **Recommended Sinks**: JDBC (XA transactions), Kafka (2PC), File connectors
- **Key Features**: Exactly-once, transaction support, state management

#### For Multi-Table Operations
- **Recommended Sources**: JDBC connectors with multi-table support
- **Recommended Sinks**: JDBC with dynamic table identifiers
- **Key Features**: Multiple table read/write, placeholder support

#### For Cloud Integration
- **Recommended Sources**: Native cloud connectors, File connectors with cloud storage
- **Recommended Sinks**: Cloud-specific connectors, File connectors
- **Examples**: S3File, OSSFile, Snowflake, Redshift, MaxCompute

#### For Advanced Analytics
- **Recommended Sources**: Data lake formats, Analytical databases
- **Recommended Sinks**: Data lake formats (Iceberg, Hudi, Paimon), OLAP databases
- **Examples**: Clickhouse, Doris, StarRocks, Druid

## Engine Compatibility Notes

### SeaTunnel Zeta (Recommended)
- **Advantages**: Best performance, most features, unified API
- **Connector Coverage**: ~82% source, ~90% sink
- **Use Cases**: Production deployments, performance-critical workloads

### Apache Flink
- **Advantages**: Stream processing excellence, fault tolerance
- **Connector Coverage**: ~95% source, ~98% sink
- **Use Cases**: Complex streaming, stateful processing

### Apache Spark
- **Advantages**: Batch processing, ecosystem integration
- **Connector Coverage**: ~90% source, ~98% sink
- **Use Cases**: Large-scale batch processing, ETL workflows

## Data Format Support

| Data Format | Source Support | Sink Support | Primary Connectors |
|-------------|----------------|---------------|------------------|
| **JSON** | ✅ Most | ✅ Most | Universal default format |
| **CSV** | ✅ File | ✅ File | LocalFile, HdfsFile, S3File |
| **Avro** | ✅ Kafka/File | ✅ Kafka/File | Kafka, File connectors |
| **Parquet** | ✅ File/Hive | ✅ File/Hive | LocalFile, HdfsFile, Hive |
| **ORC** | ✅ File/Hive | ✅ File/Hive | LocalFile, HdfsFile, Hive |
| **Text** | ✅ File/Kafka | ✅ File/Kafka | File connectors, Kafka |
| **XML** | ✅ File | ✅ File | File connectors |
| **Protobuf** | ✅ Kafka | ✅ Kafka | Kafka |
| **Canal-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **Debezium-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **Maxwell-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **OGG-JSON** | ✅ Kafka | ✅ Kafka | Kafka |

## Getting Started

### Quick Setup

1. **Choose Engine**: Select SeaTunnel Zeta for best performance
2. **Select Connectors**: Use the matrices above to choose appropriate source/sink
3. **Install Plugins**: Download required connector JAR files
4. **Configure Job**: Create configuration based on feature requirements
5. **Test & Deploy**: Validate configuration and run production jobs

### Best Practices

1. **Feature Matching**: Choose connectors that support your required features
2. **Engine Selection**: Use SeaTunnel Zeta when possible for maximum compatibility
3. **Performance**: Enable parallelism and batch processing where supported
4. **Reliability**: Prioritize exactly-once support for critical workloads
5. **Monitoring**: Monitor connector performance and adjust configurations

## Contributing

Want to add new connectors or improve existing ones? Check our:
- [Contributor Guide](../contributing.md)
- [Connector Development Guidelines](../development/connector-development.md)
- [Community Forums](https://github.com/apache/seatunnel/discussions)

---

*This matrix represents the current state of SeaTunnel connectors based on official documentation. For the most up-to-date information, refer to individual connector documentation pages. Feature availability may vary between versions.*