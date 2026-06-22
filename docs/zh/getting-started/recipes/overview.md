---
slug: /getting-started/recipes
---

# 场景示例

这些示例更适合在你已经跑通第一个本地任务之后再阅读。不要按顺序把所有示例都看一遍，而是优先找到最接近你真实 source 和 sink 组合的那条链路。

## 按业务目标选择示例

| 目标 | 推荐入口 |
| --- | --- |
| 从 MySQL CDC 同步到分析型数据库 | [MySQL CDC 到 Doris](./mysql-cdc-to-doris.md) |
| 把 JDBC 数据抽取到对象存储 | [JDBC 到 S3](./jdbc-to-s3.md) |
| 从 Kafka 流式写入表格式存储 | [Kafka 到 Iceberg](./kafka-to-iceberg.md) |
| 把 HTTP 数据写入关系型数据库 | [HTTP 到 JDBC](./http-to-jdbc.md) |
| 把文件数据加载到分析型系统 | [文件到 StarRocks](./file-to-starrocks.md) |
| 多表 CDC 编排 | [多表 CDC](./multi-table-cdc.md) |

## 阅读示例时建议这样看

1. 先确认 source 和 sink 组合与你的目标链路是否一致。
2. 再对照 `env`、`source`、`transform`、`sink` 四段结构理解参数。
3. 改造示例时，一次只替换一个系统，避免同时改太多变量。
4. 如果示例依赖 CDC、额外驱动或插件安装，先确认前置条件再运行。
