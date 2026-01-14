---
title: 连接器概览
---

# SeaTunnel 连接器能力概览

SeaTunnel 提供了一套全面的连接器，使您能够从各种数据源读取数据并写入不同的数据目标。本文档基于[连接器 V2 特性](../introduction/concepts/connector-v2-features.md)提供了所有可用连接器的详细能力矩阵。

## 快速概览

- **源连接器总数**: 79
- **目标连接器总数**: 78
- **连接器总数**: 157
- **支持的引擎**: Spark、Flink、SeaTunnel Zeta
- **支持的数据类型**: 结构化、非结构化、多模态

## 特性定义

### 源连接器特性

| 特性 | 描述 |
|---------|-------------|
| **exactly-once** | 每条数据只向下游发送一次，通过状态快照和偏移量保证可靠性 |
| **column projection** | 高效地从数据源只读取指定列 |
| **batch** | 支持有界数据处理（完成后作业停止） |
| **stream** | 支持无界数据处理（连续流式处理） |
| **parallelism** | 支持多个任务并行执行，读取不同的分片 |
| **multimodal** | 支持结构化和非结构化数据（文本、视频、图片、二进制文件） |
| **support user-defined split** | 用户可以配置自定义分片规则 |
| **support multiple table read** | 在一个 SeaTunnel 作业中读取多个表 |

### 目标连接器特性

| 特性 | 描述 |
|---------|-------------|
| **exactly-once** | 通过键去重或 XA 事务确保每条数据只写入目标一次 |
| **cdc** | 支持基于主键的变更数据捕获（INSERT/UPDATE/DELETE 操作） |
| **support multiple table write** | 在一个 SeaTunnel 作业中写入多个表，使用动态表标识符 |
| **multimodal** | 支持结构化和非结构化数据（文本、视频、图片、二进制文件） |

## 源连接器能力矩阵

### 数据库和 CDC 连接器

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
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

### NoSQL 数据库

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| MongoDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Cassandra | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Hbase | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Redis | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Neo4j | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 数据湖和仓库

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Iceberg | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Hudi | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Paimon | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Databend | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Maxcompute | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| OceanBase | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |

### 消息队列

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Kafka | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Pulsar | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| Rabbitmq | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| RocketMQ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| AmazonSqs | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |

### 文件系统

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| LocalFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| HdfsFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| S3File | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OssFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| OssJindoFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| ObsFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| CosFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| FtpFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| SftpFile | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |

### 时间序列和搜索引擎

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| InfluxDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| IoTDB | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| IoTDBv2 | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| TDengine | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Elasticsearch | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Easysearch | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Typesense | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Prometheus | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 向量数据库

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| Milvus | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Qdrant | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### API 和云服务

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
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

### 特殊和测试连接器

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | column projection | batch | stream | parallelism | multimodal | user-defined split | multiple table |
|-----------|-------|-------|----------------|--------------|------------------|-------|--------|-------------|------------|------------------|-----------------|
| FakeSource | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Cloudberry | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Kudu | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| OpenMldb | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Tablestore | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

## 目标连接器能力矩阵

### 数据库目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Jdbc** | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| **MySQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **PostgreSQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Oracle** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **SQLServer** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **DB2** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Kingbase** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Hive** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Phoenix** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Greenplum** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |

### NoSQL 和图数据库

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
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

### 数据仓库和分析型数据库

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Clickhouse** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Doris** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **StarRocks** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Redshift** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Snowflake** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Databend** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Vertica** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Druid** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 消息队列目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Kafka** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Pulsar** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Rabbitmq** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **RocketMQ** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **AmazonSqs** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Activemq** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 文件系统目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
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

### 数据湖目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Iceberg** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Hudi** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Paimon** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 搜索和时间序列目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Elasticsearch** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Easysearch** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Typesense** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **InfluxDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **IoTDB** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **IoTDBv2** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **TDengine** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Prometheus** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 向量数据库目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Milvus** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Qdrant** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### API 和云服务目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **Http** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **GraphQL** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Socket** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Datahub** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Maxcompute** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

### 专业化目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
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

### 其他目标

| 连接器 | Spark | Flink | SeaTunnel Zeta | exactly-once | cdc | multiple table | multimodal |
|-----------|-------|-------|----------------|--------------|-----|----------------|------------|
| **S3-Redshift** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **SelectDB-Cloud** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Tablestore** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Kudu** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Cloudberry** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Fluss** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **OceanBase** | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

## 特性支持总结

### 源连接器特性分布

| 特性 | 数量 | 百分比 | 示例 |
|--------|-------|------------|----------|
| **引擎支持** | | | |
| 三引擎全支持 | ~65 | 82% | 大多数数据库、文件和消息队列连接器 |
| 仅 Spark + Flink | ~8 | 10% | 一些专业化 API 连接器 |
| 仅 Flink + Zeta | ~4 | 5% | CDC 连接器 |
| **处理模式** | | | |
| 批处理支持 | ~60 | 76% | 大多数数据库和文件连接器 |
| 流处理支持 | ~70 | 89% | 大多数连接器，特别是 CDC 和消息队列 |
| **可靠性** | | | |
| Exactly-Once | ~45 | 57% | 文件连接器、JDBC、Kafka |
| **性能** | | | |
| 并行度 | ~55 | 70% | 大多数数据库和文件连接器 |
| 列投影 | ~25 | 32% | JDBC、文件、一些专业化连接器 |
| **高级特性** | | | |
| 用户自定义分片 | ~15 | 19% | CDC、一些文件连接器 |
| 多表读取 | ~25 | 32% | JDBC 和一些数据库连接器 |
| 多模态支持 | ~10 | 13% | 文件和一些专业化连接器 |

### 目标连接器特性分布

| 特性 | 数量 | 百分比 | 示例 |
|--------|-------|------------|----------|
| **引擎支持** | | | |
| 三引擎全支持 | ~70 | 90% | 大多数数据库和文件连接器 |
| 仅 Spark + Flink | ~6 | 8% | 一些专业化连接器 |
| 仅 Flink + Zeta | ~2 | 3% | 专业化场景 |
| **可靠性** | | | |
| Exactly-Once | ~40 | 51% | JDBC、Kafka、文件连接器 |
| **数据能力** | | | |
| CDC 支持 | ~5 | 6% | 仅限于专业化数据库目标 |
| 多表写入 | ~15 | 19% | JDBC 和一些数据库目标 |
| 多模态支持 | ~10 | 13% | 文件和专业化连接器 |

## 连接器选择指南

### 基于用例的建议

#### 高吞吐量批处理
- **推荐源**: Jdbc、文件连接器（LocalFile、HdfsFile、S3File）
- **推荐目标**: Jdbc、文件连接器、数据湖格式（Iceberg、Hudi）
- **关键特性**: 批处理支持、并行度、列投影

#### 实时流处理
- **推荐源**: CDC 连接器、Kafka、文件连接器（流模式）
- **推荐目标**: Kafka、Jdbc（支持事务）、实时数据库
- **关键特性**: 流处理支持、exactly-once、低延迟

#### Exactly-Once 保证
- **推荐源**: 文件连接器、JDBC、Kafka
- **推荐目标**: JDBC（XA 事务）、Kafka（2PC）、文件连接器
- **关键特性**: Exactly-once、事务支持、状态管理

#### 多表操作
- **推荐源**: 支持多表的 JDBC 连接器
- **推荐目标**: 支持动态表标识符的 JDBC
- **关键特性**: 多表读写、占位符支持

#### 云集成
- **推荐源**: 原生云连接器、支持云存储的文件连接器
- **推荐目标**: 云专用连接器、文件连接器
- **示例**: S3File、OSSFile、Snowflake、Redshift、MaxCompute

#### 高级分析
- **推荐源**: 数据湖格式、分析型数据库
- **推荐目标**: 数据湖格式（Iceberg、Hudi、Paimon）、OLAP 数据库
- **示例**: Clickhouse、Doris、StarRocks、Druid

## 引擎兼容性说明

### SeaTunnel Zeta（推荐）
- **优势**: 最佳性能、最多特性、统一 API
- **连接器覆盖**: ~82% 源，~90% 目标
- **用例**: 生产部署、性能关键型工作负载

### Apache Flink
- **优势**: 流处理卓越、容错性
- **连接器覆盖**: ~95% 源，~98% 目标
- **用例**: 复杂流处理、有状态处理

### Apache Spark
- **优势**: 批处理、生态系统集成
- **连接器覆盖**: ~90% 源，~98% 目标
- **用例**: 大规模批处理、ETL 工作流

## 数据格式支持

| 数据格式 | 源支持 | 目标支持 | 主要连接器 |
|-------------|----------------|---------------|------------------|
| **JSON** | ✅ 大多数 | ✅ 大多数 | 通用默认格式 |
| **CSV** | ✅ 文件 | ✅ 文件 | LocalFile、HdfsFile、S3File |
| **Avro** | ✅ Kafka/文件 | ✅ Kafka/文件 | Kafka、文件连接器 |
| **Parquet** | ✅ 文件/Hive | ✅ 文件/Hive | LocalFile、HdfsFile、Hive |
| **ORC** | ✅ 文件/Hive | ✅ 文件/Hive | LocalFile、HdfsFile、Hive |
| **Text** | ✅ 文件/Kafka | ✅ 文件/Kafka | 文件连接器、Kafka |
| **XML** | ✅ 文件 | ✅ 文件 | 文件连接器 |
| **Protobuf** | ✅ Kafka | ✅ Kafka | Kafka |
| **Canal-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **Debezium-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **Maxwell-JSON** | ✅ Kafka | ✅ Kafka | Kafka |
| **OGG-JSON** | ✅ Kafka | ✅ Kafka | Kafka |

## 快速开始

### 快速设置

1. **选择引擎**: 选择 SeaTunnel Zeta 以获得最佳性能
2. **选择连接器**: 使用上述矩阵选择合适的源/目标
3. **安装插件**: 下载所需的连接器 JAR 文件
4. **配置作业**: 基于特性要求创建配置
5. **测试和部署**: 验证配置并运行生产作业

### 最佳实践

1. **特性匹配**: 选择支持所需特性的连接器
2. **引擎选择**: 尽可能使用 SeaTunnel Zeta 以获得最大兼容性
3. **性能**: 在支持的地方启用并行度和批处理
4. **可靠性**: 为关键工作负载优先考虑 exactly-once 支持
5. **监控**: 监控连接器性能并调整配置

## 贡献

想要添加新连接器或改进现有连接器？请查看：
- [贡献者指南](../developer/contribution-guide.md)
- [连接器开发指南](../developer/connector-development)
- [社区论坛](https://github.com/apache/seatunnel/discussions)

---

*此矩阵基于官方文档反映了 SeaTunnel 连接器的当前状态。要获取最新信息，请参考各个连接器的文档页面。特性可用性可能因版本而异。*