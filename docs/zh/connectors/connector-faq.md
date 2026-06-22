---
sidebar_position: 10
---

# 连接器常见问题

这一页按连接器类别整理常见问题入口。每一项都会跳转到对应连接器文档中的常见问题部分。

这些入口只用于快速导航，不作为第二份参数说明。准确的参数名、默认值和完整示例，请以各连接器页面里的参数表和详细说明为准。

SeaTunnel 引擎配置、变量替换、调度等通用问题，请参考[通用常见问题](../faq.md)。

---

## CDC 连接器

CDC 连接器会从数据库事务日志中读取实时变更事件，例如 INSERT、UPDATE 和 DELETE。

| 连接器 | 常见问题主题 |
|---|---|
| [MySQL CDC](./source/MySQL-CDC.md#常见问题) | 所需权限、binlog 设置、备库支持、无主键表、快照阶段、DDL 传播、`server-id` 冲突、快照性能、时区和字符集 |
| [PostgreSQL CDC](./source/PostgreSQL-CDC.md#常见问题) | 所需权限、逻辑解码插件、备库支持、无主键表、复制槽管理、复制延迟 |
| [Oracle CDC](./source/Oracle-CDC.md#常见问题) | LogMiner 权限、补充日志、CDB/PDB 多租户、无主键表、LogMiner 性能、支持的 Oracle 版本 |

---

## 消息队列连接器

| 连接器 | 常见问题主题 |
|---|---|
| [Kafka Source](./source/Kafka.md#常见问题) | `start_mode` 选项、按消息 key 过滤、支持的格式、SASL/Kerberos 认证、消费组 offset 提交 |
| [Kafka Sink](./sink/Kafka.md#常见问题) | 自动创建 topic、`partition_key_fields` 行为、精确一次投递、SASL/Kerberos 认证、支持的格式 |

---

## Sink 连接器

### OLAP / 分析型存储

| 连接器 | 常见问题主题 |
|---|---|
| [Doris Sink](./sink/Doris.md#常见问题) | 自动建表、基于 2PC 的精确一次、"Label already exists" 报错、DELETE 传播、列名大小写、Stream Load 格式 |
| [StarRocks Sink](./sink/StarRocks.md#常见问题) | 自动建表、upsert 和 DELETE 支持、`labelPrefix` 用法、列名大小写、`nodeUrls` 与 `base-url` |
| [ClickHouse Sink](./sink/Clickhouse.md#常见问题) | 自动建表、批量写入性能、支持的数据类型、"Table doesn't exist" 报错 |

### 关系型数据库

| 连接器 | 常见问题主题 |
|---|---|
| [JDBC Sink](./sink/Jdbc.md#常见问题) | 自动建表、基于 XA 事务的精确一次、upsert / 主键配置、多表写入、缺少 JDBC 驱动 |

### 数据湖 / 文件系统

| 连接器 | 常见问题主题 |
|---|---|
| [Hive Sink](./sink/Hive.md#常见问题) | 支持的文件格式、分区表、Kerberos 认证、小文件问题、Schema 演进 |

---

## 如何快速找到答案

1. **某个连接器的问题**：直接进入对应连接器页面，查看页面里的“常见问题”部分。
2. **跨连接器通用问题**：例如 SeaTunnel 是否支持 CDC、`schema_save_mode` 是什么，请查看[通用常见问题](../faq.md)。
3. **仍然无法解决**：可以搜索 [GitHub Issues](https://github.com/apache/seatunnel/issues)，或通过[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)联系社区。
