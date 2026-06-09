---
sidebar_position: 10
---

# Connector 常见问题

本页面是按 Connector 类别组织的常见问题索引，每个条目均链接到对应 Connector 文档页面的 FAQ 小节。

这些 FAQ 的定位是“快速导航”，不是另一套独立事实源。涉及精确配置项名称、默认值和完整示例时，应以
各 Connector 页面中的 option 表和对应详细章节为准。

关于 SeaTunnel 通用问题（引擎部署、变量替换、调度等），请查阅[通用 FAQ](../faq.md)。

---

## CDC 类 Connector

CDC（变更数据捕获）Connector 从数据库事务日志中实时读取 INSERT / UPDATE / DELETE 变更事件。

| Connector | 常见问题主题 |
|---|---|
| [MySQL CDC](./source/MySQL-CDC.md#常见问题) | 所需权限、binlog 配置、从库支持、无主键表、快照阶段、DDL 传播、server-id 冲突、快照性能、时区/字符集 |
| [PostgreSQL CDC](./source/PostgreSQL-CDC.md#常见问题) | 所需权限、逻辑解码插件、备库限制、无主键表、复制槽管理、复制滞后 |
| [Oracle CDC](./source/Oracle-CDC.md#常见问题) | LogMiner 权限、附加日志、CDB/PDB 多租户、无主键表、LogMiner 性能、支持的 Oracle 版本 |

---

## 消息队列类 Connector

| Connector | 常见问题主题 |
|---|---|
| [Kafka Source](./source/Kafka.md#常见问题) | `start_mode` 各取值对比、按消息 key 过滤、支持的格式、SASL/Kerberos 认证、消费组 offset 提交 |
| [Kafka Sink](./sink/Kafka.md#常见问题) | 自动创建 topic（Broker 端行为说明）、`partition_key_fields` 为空时的行为、精确一次、SASL/Kerberos 认证、支持的格式 |

---

## Sink 类 Connector

### OLAP / 分析型存储

| Connector | 常见问题主题 |
|---|---|
| [Doris Sink](./sink/Doris.md#常见问题) | 自动建表、2PC 精确一次、"Label already exists" 报错、DELETE 传播、列名大小写、Stream Load 格式 |
| [StarRocks Sink](./sink/StarRocks.md#常见问题) | 自动建表、Upsert/DELETE 支持、`labelPrefix` 用法、列名大小写、`nodeUrls` 与 `base-url` 的区别 |
| [ClickHouse Sink](./sink/Clickhouse.md#常见问题) | 自动建表、批量写入调优、支持的数据类型、"Table doesn't exist" 报错 |

### 关系型数据库

| Connector | 常见问题主题 |
|---|---|
| [JDBC Sink](./sink/Jdbc.md#常见问题) | 自动建表、XA 精确一次、Upsert/主键配置、多表写入、JDBC 驱动未找到 |

### 数据湖 / 文件系统

| Connector | 常见问题主题 |
|---|---|
| [Hive Sink](./sink/Hive.md#常见问题) | 支持的文件格式、分区表、Kerberos 认证、小文件问题、Schema 演变 |

---

## 找答案的建议

1. **Connector 特定问题** → 直接进入对应 Connector 页面，滚动到 **FAQ** 小节。
2. **跨 Connector 通用问题**（如"SeaTunnel 是否支持 CDC？""什么是 `schema_save_mode`？"）→ 查阅[通用 FAQ](../faq.md)。
3. **仍未解决？** → 搜索 [GitHub Issues](https://github.com/apache/seatunnel/issues) 或通过[邮件列表](https://lists.apache.org/list.html?dev@seatunnel.apache.org)联系社区。
