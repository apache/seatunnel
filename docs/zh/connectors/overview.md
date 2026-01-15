---
sidebar_position: 1
---

# 连接器概览

SeaTunnel 提供了 100+ 个连接器用于数据集成。

## 快速统计

| 指标 | 数量 |
|------|------|
| Source 连接器 | 79 |
| Sink 连接器 | 78 |
| CDC 连接器 | 7 |
| 支持的引擎 | SeaTunnel Zeta, Flink, Spark |

## 所有连接器

> **维护指南**: 添加新连接器时，只需在对应表格中按字母顺序添加一行即可。

### 图例

| 符号 | 含义 |
|------|------|
| ✅ | 支持 |
| ❌ | 不支持 |
| S | Source（数据源） |
| K | Sink（目标端） |

---

## 数据库连接器

| 连接器 | S | K | CDC | 批处理 | 流处理 | 精确一次 | 多表 | 文档 |
|--------|---|---|-----|--------|--------|----------|------|------|
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

## CDC 连接器

> CDC 连接器用于捕获实时数据变更。**注意**: CDC 连接器不支持 Spark 引擎。

| 连接器 | 精确一次 | 并行度 | 多表 | Schema 演变 | 文档 |
|--------|----------|--------|------|-------------|------|
| MongoDB-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/MongoDB-CDC.md) |
| MySQL-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/MySQL-CDC.md) |
| Opengauss-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/Opengauss-CDC.md) |
| Oracle-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/Oracle-CDC.md) |
| PostgreSQL-CDC | ✅ | ✅ | ✅ | ✅ | [Source](source/PostgreSQL-CDC.md) |
| SqlServer-CDC | ✅ | ✅ | ✅ | ❌ | [Source](source/SqlServer-CDC.md) |
| TiDB-CDC | ✅ | ✅ | ❌ | ❌ | [Source](source/TiDB-CDC.md) |

---

## NoSQL 连接器

| 连接器 | S | K | 批处理 | 流处理 | 精确一次 | 文档 |
|--------|---|---|--------|--------|----------|------|
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

## 数据湖连接器

| 连接器 | S | K | 精确一次 | CDC 支持 | 多表 | 文档 |
|--------|---|---|----------|----------|------|------|
| Fluss | ❌ | ✅ | ❌ | ✅ | ✅ | [Sink](sink/Fluss.md) |
| Hudi | ❌ | ✅ | ❌ | ✅ | ❌ | [Sink](sink/Hudi.md) |
| Iceberg | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Iceberg.md) / [Sink](sink/Iceberg.md) |
| Paimon | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/Paimon.md) / [Sink](sink/Paimon.md) |

---

## 消息队列连接器

| 连接器 | S | K | 批处理 | 流处理 | 精确一次 | 文档 |
|--------|---|---|--------|--------|----------|------|
| Activemq | ❌ | ✅ | - | - | ❌ | [Sink](sink/Activemq.md) |
| AmazonSqs | ✅ | ✅ | ❌ | ✅ | ❌ | [Source](source/AmazonSqs.md) / [Sink](sink/AmazonSqs.md) |
| Kafka | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/Kafka.md) / [Sink](sink/Kafka.md) |
| Pulsar | ✅ | ✅ | ❌ | ✅ | ✅ | [Source](source/Pulsar.md) / [Sink](sink/Pulsar.md) |
| Rabbitmq | ✅ | ✅ | ❌ | ✅ | ❌ | [Source](source/Rabbitmq.md) / [Sink](sink/Rabbitmq.md) |
| RocketMQ | ✅ | ✅ | ✅ | ✅ | ✅ | [Source](source/RocketMQ.md) / [Sink](sink/RocketMQ.md) |

---

## 文件系统连接器

> 所有文件连接器支持格式: JSON, CSV, Parquet, ORC, Text, XML, Excel, Binary

| 连接器 | S | K | 精确一次 | 多表 | 多模态 | 文档 |
|--------|---|---|----------|------|--------|------|
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

## 搜索引擎连接器

| 连接器 | S | K | 批处理 | 流处理 | CDC 支持 | 文档 |
|--------|---|---|--------|--------|----------|------|
| Easysearch | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Easysearch.md) / [Sink](sink/Easysearch.md) |
| Elasticsearch | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Elasticsearch.md) / [Sink](sink/Elasticsearch.md) |
| Typesense | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/Typesense.md) / [Sink](sink/Typesense.md) |

---

## 时序数据库连接器

| 连接器 | S | K | 批处理 | 流处理 | 精确一次 | 文档 |
|--------|---|---|--------|--------|----------|------|
| InfluxDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/InfluxDB.md) / [Sink](sink/InfluxDB.md) |
| IoTDB | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/IoTDB.md) / [Sink](sink/IoTDB.md) |
| IoTDBv2 | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/IoTDBv2.md) / [Sink](sink/IoTDBv2.md) |
| Prometheus | ✅ | ✅ | ✅ | ❌ | ❌ | [Source](source/Prometheus.md) / [Sink](sink/Prometheus.md) |
| TDengine | ✅ | ✅ | ✅ | ❌ | ✅ | [Source](source/TDengine.md) / [Sink](sink/TDengine.md) |

---

## 向量数据库连接器

| 连接器 | S | K | 批处理 | 流处理 | 文档 |
|--------|---|---|--------|--------|------|
| Milvus | ✅ | ✅ | ✅ | ❌ | [Source](source/Milvus.md) / [Sink](sink/Milvus.md) |
| Qdrant | ✅ | ✅ | ✅ | ❌ | [Source](source/Qdrant.md) / [Sink](sink/Qdrant.md) |

---

## API & HTTP 连接器

| 连接器 | S | K | 批处理 | 流处理 | 文档 |
|--------|---|---|--------|--------|------|
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

## 云服务连接器

| 连接器 | S | K | 描述 | 文档 |
|--------|---|---|------|------|
| Datahub | ❌ | ✅ | LinkedIn DataHub | [Sink](sink/Datahub.md) |
| S3-Redshift | ❌ | ✅ | 通过 S3 写入 Redshift | [Sink](sink/S3-Redshift.md) |
| Sls | ✅ | ✅ | 阿里云日志服务 | [Source](source/Sls.md) / [Sink](sink/Sls.md) |
| Tablestore | ✅ | ✅ | 阿里云表格存储 | [Source](source/Tablestore.md) / [Sink](sink/Tablestore.md) |

---

## 通知连接器

| 连接器 | 描述 | 文档 |
|--------|------|------|
| DingTalk | 钉钉机器人 | [Sink](sink/DingTalk.md) |
| Email | 发送邮件 | [Sink](sink/Email.md) |
| Enterprise-WeChat | 企业微信机器人 | [Sink](sink/Enterprise-WeChat.md) |
| Feishu | 飞书机器人 | [Sink](sink/Feishu.md) |
| Sentry | Sentry 事件 | [Sink](sink/Sentry.md) |
| Slack | Slack 消息 | [Sink](sink/Slack.md) |

---

## 工具连接器

| 连接器 | 类型 | 描述 | 文档 |
|--------|------|------|------|
| Assert | Sink | 数据验证测试 | [Sink](sink/Assert.md) |
| Console | Sink | 打印到控制台 | [Sink](sink/Console.md) |
| FakeSource | Source | 生成测试数据 | [Source](source/FakeSource.md) |

---

## 其他连接器

| 连接器 | S | K | 描述 | 文档 |
|--------|---|---|------|------|
| ClickhouseFile | ❌ | ✅ | 写入 Clickhouse 本地文件 | [Sink](sink/ClickhouseFile.md) |
| Kudu | ✅ | ✅ | Apache Kudu | [Source](source/Kudu.md) / [Sink](sink/Kudu.md) |
| OpenMldb | ✅ | ❌ | OpenMLDB 特征平台 | [Source](source/OpenMldb.md) |
| SensorsData | ❌ | ✅ | 神策分析 | [Sink](sink/SensorsData.md) |

---

## 特性说明

| 特性 | 描述 |
|------|------|
| **S** | 支持 Source 连接器 |
| **K** | 支持 Sink 连接器 |
| **批处理** | 有界数据处理 |
| **流处理** | 无界持续处理 |
| **精确一次** | 每条记录仅处理一次 |
| **CDC 支持** | 处理 INSERT/UPDATE/DELETE 操作 |
| **多表** | 单作业处理多张表 |
| **多模态** | 支持二进制/非结构化数据 |
| **Schema 演变** | 自动处理 Schema 变更 |

详细特性定义请参考 [Connector V2 特性](../introduction/concepts/connector-v2-features.md)。

## 相关文档

- [Source 通用选项](common-options/source-common-options.md)
- [Sink 通用选项](common-options/sink-common-options.md)
- [数据格式](formats/avro.md)
- [连接器变更日志](changelog/connector-common.md)
