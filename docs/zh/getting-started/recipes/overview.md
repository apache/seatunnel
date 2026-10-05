---
slug: /getting-started/recipes
---

# 场景教程

这些示例更适合在你已经跑通第一个本地任务之后再阅读。不要按顺序把所有示例都看一遍，而是优先找到最接近你真实 source 和 sink 组合的那条链路。

本节教程提供了明确的前置条件、完整配置和预期结果，便于你在自己的环境中逐项验证整条链路。

## 按业务目标选择示例

| 目标 | 推荐入口 |
| --- | --- |
| 从 MySQL CDC 数据实时同步到 Kafka 并附带元数据消息头 | [MySQL CDC 到 Kafka](./mysql-cdc-to-kafka.md) |
| 从 MySQL CDC 数据实时同步到 Elasticsearch 并完成过滤与字段整形 | [MySQL CDC 到 Elasticsearch](./mysql-cdc-to-elasticsearch.md) |
| 从 JDBC 数据批量同步到 JDBC 并进行数据过滤和转换 | [JDBC 到 JDBC](./jdbc-to-jdbc.md) |
| 从 JDBC 数据批量同步到 S3 对象存储 | [JDBC 到 S3](./jdbc-to-s3.md) |
| 从 MySQL 批量写入 HDFS，生成按日期分区的 Parquet 文件 | [MySQL 到 HDFS](./mysql-to-hdfs.md) |
| 从 Kafka 流式写入 Iceberg | [Kafka 到 Iceberg](./kafka-to-iceberg.md) |
| 从 PostgreSQL CDC 数据实时同步到 Iceberg | [PostgreSQL CDC 到 Iceberg](./postgresql-cdc-to-iceberg.md) |
| 从 HTTP 数据批量写入 JDBC 关系型数据库 | [HTTP 到 JDBC](./http-to-jdbc.md) |
| 从 MySQL CDC 数据实时同步到 Doris | [MySQL CDC 到 Doris](./mysql-cdc-to-doris.md) |
| 从 File 文件数据同步到 StarRocks| [文件到 StarRocks](./file-to-starrocks.md) |
| CDC 实时多表数据同步 | [多表 CDC](./multi-table-cdc.md) |

## 阅读示例时建议这样看

1. 先确认 source 和 sink 组合与你的目标链路是否一致。
2. 再对照 `env`、`source`、`transform`、`sink` 四段结构理解参数。
3. 改造示例时，一次只替换一个系统，避免同时改太多变量。
4. 如果示例依赖 CDC、额外驱动或插件安装，先确认前置条件再运行。
