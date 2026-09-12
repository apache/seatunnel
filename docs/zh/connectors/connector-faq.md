---
sidebar_position: 10
---

# 连接器常见问题

本页按连接器类别整理了常见问题的索引。每条目会链接到对应连接器文档页面内的 FAQ 章节。

这些 FAQ 章节主要用于快速导航，并非第二套权威说明。如需准确的参数名、默认值和完整示例，请以连接器页面中的参数表及链接的详细章节为准。

关于 SeaTunnel 的通用问题（引擎部署、变量替换、调度等），请参阅[通用常见问题](../faq.md)。

---

## CDC 连接器

CDC（Change Data Capture）连接器从数据库事务日志中读取实时变更事件（INSERT / UPDATE / DELETE）。

| 连接器 | 常见 FAQ 主题 |
|---|---|
| [MySQL CDC](./source/MySQL-CDC.md#faq) | 所需权限、binlog 配置、从库支持、无主键表、快照阶段、DDL 传播、`server-id` 冲突、快照性能、时区/字符集 |
| [PostgreSQL CDC](./source/PostgreSQL-CDC.md#faq) | 所需权限、逻辑解码插件、从库支持、无主键表、复制槽管理、复制延迟 |
| [Oracle CDC](./source/Oracle-CDC.md#faq) | LogMiner 权限、补充日志、CDB/PDB 多租户、无主键表、LogMiner 性能、支持的 Oracle 版本 |

---

## 消息队列连接器

| 连接器 | 常见 FAQ 主题 |
|---|---|
| [Kafka Source](./source/Kafka.md#faq) | `start_mode` 选项、按消息 key 过滤、支持的格式、SASL/Kerberos 认证、消费组 offset 提交 |
| [Kafka Sink](./sink/Kafka.md#faq) | 自动创建 Topic、`partition_key_fields` 行为、精确一次投递、SASL/Kerberos 认证、支持的格式 |

---

## Sink 连接器

### OLAP / 分析型存储

| 连接器 | 常见 FAQ 主题 |
|---|---|
| [Doris Sink](./sink/Doris.md#faq) | 自动建表、2PC 精确一次、"Label already exists" 错误、DELETE 传播、列名大小写、Stream Load 格式 |
| [StarRocks Sink](./sink/StarRocks.md#faq) | 自动建表、Upsert 与 DELETE 支持、`labelPrefix` 用法、列名大小写、`nodeUrls` 与 `base-url` |
| [ClickHouse Sink](./sink/Clickhouse.md#faq) | 自动建表、批量写入性能、支持的数据类型、"Table doesn't exist" 错误 |

### 关系型数据库

| 连接器 | 常见 FAQ 主题 |
|---|---|
| [JDBC Sink](./sink/Jdbc.md#faq) | 自动建表、XA 事务精确一次、Upsert / 主键配置、多表写入、缺少 JDBC 驱动 |

### 数据湖 / 文件系统

| 连接器 | 常见 FAQ 主题 |
|---|---|
| [Hive Sink](./sink/Hive.md#faq) | 支持的文件格式、分区表、Kerberos 认证、小文件问题、Schema 演进 |

---

## 查找答案的技巧

1. **连接器相关问题** → 直接进入对应连接器页面，滚动到 **FAQ** 章节。
2. **类型与 Schema 问题**（例如 JSON schema、Debezium schema、Oracle CLOB、MySQL TINYINT、ORC/Parquet 映射）→ 参阅[类型与 Schema 常见问题](./type-schema-faq.md)。
3. **文件与对象存储问题**（例如 MinIO/S3、小文件、save mode、多表文件路径）→ 参阅[文件与对象存储常见问题](./file-object-storage-faq.md)。
4. **跨连接器主题**（例如「SeaTunnel 是否支持 CDC？」「`schema_save_mode` 是什么？」）→ 参阅[通用常见问题](../faq.md)。
5. **仍未解决？** → 在 [GitHub Issues](https://github.com/apache/seatunnel/issues) 中搜索，或通过[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)联系社区。
2. **跨 Sink 写入主题**（例如「`generate_sink_sql` 和 `query` 怎么选？」「`schema_save_mode` 是什么？」「`data_save_mode` 是否支持？」「`enable_upsert` 什么时候生效？」）→ 参阅 [Sink 写入模式与 Save Mode](./common-options/sink-write-modes.md)。
3. **其他跨连接器主题**（例如「SeaTunnel 是否支持 CDC？」）→ 参阅[通用常见问题](../faq.md)。
4. **仍未解决？** → 在 [GitHub Issues](https://github.com/apache/seatunnel/issues) 中搜索，或通过[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)联系社区。
