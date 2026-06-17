---
sidebar_position: 16
---

# 多表 Transform 能力边界

## 概述

SeaTunnel 的**多表 Transform**功能允许单个 Transform 节点在一条流水线中同时处理来自上游
数据源（通常是 CDC 连接器）的多张表。本文档精确描述了哪些功能受支持、哪些不受支持，以及
遇到能力边界时的替代方案。

---

## 1. 什么是多表 Transform？

在标准的单表流水线中，一个 Source 对应一条 Transform 链，再连接一个 Sink。在多表流水线中，
单个 Source（例如 MySQL-CDC）同时发送来自**多张表**的记录，下游的每个 Transform 或 Sink
需声明它适用于哪张（些）表。

```
MySQL-CDC ──► FieldMapper (orders 表) ──► Kafka Sink (orders topic)
             │
             ├──► FieldMapper (users 表) ──► Kafka Sink (users topic)
             │
             └──► (未匹配的表直接透传) ──► Elasticsearch Sink
```

---

## 2. 能力边界一览

| 能力 | 是否支持 | 说明 |
|---|---|---|
| 按表字段重命名 / 映射 | ✅ 支持 | 使用带 `plugin_input` 和 `table_match_regex` 的 `FieldMapper` |
| 按表列过滤 | ✅ 支持 | 使用带 `plugin_input` 和 `table_match_regex` 的 `Filter` |
| 按表类型转换 | ✅ 支持 | 使用 `FieldMapper` 的 `define_sink_type` 选项 |
| 单表 SQL Transform | ✅ 支持 | 使用带 `plugin_input` 的 `SQL` Transform；必要时再用 `table_match_regex` 限定表范围 |
| `TableMerge`——将多表合并为一张 | ✅ 支持 | 各表需具有兼容的 Schema |
| `TableRename`——重命名流中的表 | ✅ 支持 | 适合按表名路由到 Sink |
| 行级过滤（按 `rowkind` 过滤） | ✅ 支持 | 使用 `FilterRowKind` Transform |
| 跨表 SQL JOIN | ❌ 不支持 | 参见第 4 节替代方案 |
| 多张 CDC 表的聚合 | ❌ 不支持 | 在下游 OLAP 引擎中执行聚合 |
| 通过 JOIN 生成新表 | ❌ 不支持 | 使用专用 SQL 引擎 |
| 通配符将同一 Transform 应用于所有表 | ⚠️ 部分支持 | 先用 `TableMerge` 合并，再做单 Transform；Schema 需兼容 |
| 流中 Schema 变更（DDL 事件） | ⚠️ 有限支持 | 取决于 Sink；部分 Sink 支持 Schema 演化，Transform 层不支持 |
| 按表 JSON 嵌套字段提取 | ✅ 支持 | 使用带 `plugin_input` 和 `table_match_regex` 的 `JsonPath` Transform |

---

## 3. TableMerge 与 SQL Join 的区别

这是最容易混淆的两个特性。

### 3.1 `TableMerge` Transform

`TableMerge` **将多张表的行流合并为一张结果表**。所有输入表必须具有相同（或兼容）的
Schema。通常用于将多表路由至同一个 Sink。

```json
{
  "plugin_name": "TableMerge",
  "plugin_input": ["orders_2023", "orders_2024"],
  "plugin_output": "all_orders",
  "merge_by_field": true
}
```

**适用场景**：多个来源表结构相同，需要合并（例如分片表、多年分区表，或多库同构表）。

**不适用场景**：需要关联或补全不同结构的表数据，这类场景需要 JOIN。

### 3.2 SQL JOIN（多表流水线中不支持）

SQL JOIN 基于关联键将两张不同表的行进行关联。**SeaTunnel 的 `SQL` Transform 在多表流式
流水线中不支持跨表 JOIN。** 在单个 SQL Transform 中尝试关联来自两张上游表的记录会导致
配置报错。

**推荐替代方案**：

- 将两张表写入共享数据湖或数仓（如 Hudi、Iceberg、ClickHouse），在目标端执行 JOIN
- 使用 Apache Flink 配合 SeaTunnel Flink 连接器进行有状态 JOIN
- 将“维度表”物化到查找缓存（如 Redis、RocksDB）中，通过自定义 Transform 进行数据补全

---

## 4. 跨 Source JOIN 的限制

SeaTunnel **不支持**两个输入侧来自**不同 Source** 的流式 JOIN（例如将 MySQL-CDC 与
PostgreSQL-CDC 流进行 JOIN）。

| 场景 | 是否支持 |
|---|---|
| 单 Source 多表透传 | ✅ |
| 单 Source TableMerge（相同 Schema） | ✅ |
| 跨 Source JOIN（MySQL-CDC + PG-CDC） | ❌ |
| 跨 Source JOIN（CDC + JDBC 批量） | ❌ |
| 同 Source 两张不同表的 JOIN | ❌ |

**解决方法**：将两个 Source 的数据写入公共 Sink（Kafka、Iceberg 等），再在专用 SQL 引擎
（Flink、Spark、ClickHouse 等）中执行 JOIN。

---

## 5. 按表独立配置 Transform 的模式

当你需要对同一 Source 的不同表应用不同的 Transform 时，可声明多个共享相同
`plugin_input`、但使用不同 `table_match_regex` 的 Transform 块：

```json
{
  "env": {
    "job.name": "cdc-multi-table",
    "job.mode": "STREAMING"
  },
  "source": [
    {
      "plugin_name": "MySQL-CDC",
      "plugin_output": "cdc_stream",
      "base-url": "jdbc:mysql://localhost:3306/mydb",
      "username": "cdc_user",
      "password": "password",
      "database-names": ["mydb"],
      "table-names": ["mydb.orders", "mydb.users", "mydb.products"]
    }
  ],
  "transform": [
    {
      "plugin_name": "FieldMapper",
      "plugin_input": ["cdc_stream"],
      "plugin_output": "orders_mapped",
      "field_mapper": {
        "order_id": "id",
        "order_amount": "amount"
      },
      "table_match_regex": "mydb\\.orders"
    },
    {
      "plugin_name": "FieldMapper",
      "plugin_input": ["cdc_stream"],
      "plugin_output": "users_mapped",
      "field_mapper": {
        "user_id": "id",
        "user_email": "email"
      },
      "table_match_regex": "mydb\\.users"
    }
  ],
  "sink": [
    {
      "plugin_name": "Kafka",
      "plugin_input": ["orders_mapped"],
      "topic": "orders"
    },
    {
      "plugin_name": "Kafka",
      "plugin_input": ["users_mapped"],
      "topic": "users"
    },
    {
      "plugin_name": "Kafka",
      "plugin_input": ["cdc_stream"],
      "topic": "products",
      "table_match_regex": "mydb\\.products"
    }
  ]
}
```

---

## 6. 公共字段示例（共享 Schema）

如果多张表共享一组公共字段，可以先用 `TableMerge` 合并，再统一应用一个 Transform：

```json
"transform": [
  {
    "plugin_name": "TableMerge",
    "plugin_input": ["cdc_stream"],
    "plugin_output": "all_events",
    "table_match_regex": "mydb\\.(orders|payments|refunds)"
  },
  {
    "plugin_name": "FieldMapper",
    "plugin_input": ["all_events"],
    "plugin_output": "all_events_mapped",
    "field_mapper": {
      "created_at": "event_time",
      "event_type": "type"
    }
  }
]
```

此模式仅在三张表（`orders`、`payments`、`refunds`）都包含 `created_at` 和
`event_type` 字段时有效。若各表 Schema 不同，`TableMerge` 会在运行时报错。

---

## 7. 多表 Transform 的 EtLT 模式

**EtLT**（Extract、轻量 transform、Load，再在数仓中 Transform）是当 SeaTunnel
Transform 层无法完成全量转换需求时的推荐架构模式：

```
CDC Source
   │
   ▼
轻量 Transform（字段重命名、类型转换、行过滤）
   │
   ▼
数据湖 / 数仓（Hudi / Iceberg / ClickHouse）
   │
   ▼
重型 Transform（JOIN、聚合、复杂 SQL）
在 dbt / Flink SQL / Spark SQL 中执行
```

**适合在 SeaTunnel Transform 层完成的操作**：

- 字段重命名 / 过滤
- 类型标准化
- 行级过滤
- Schema 路由（不同表 -> 不同 Topic / 表）

**建议下沉到下游处理的操作**：

- 跨表 JOIN
- 聚合计算
- Pivot / Unpivot
- 基于维度表的数据补全

---

## 8. 常见问题

**Q: 我能否不指定每张表，直接将一个 Transform 应用于所有表？**

不能直接实现。可以先用 `TableMerge` 将 Schema 兼容的表合并，再对合并结果应用单一
Transform。若各表 Schema 不同，则必须拆成多个 Transform 块，通常通过不同的
`table_match_regex` 分别处理。

**Q: `SQL` Transform 支持 `JOIN` 吗？**

不支持。`SQL` Transform 仅支持单表查询（SELECT、WHERE、表达式等）。如需执行 JOIN，
请先将数据加载至 Sink，再使用外部 SQL 引擎处理。

**Q: 未被任何 Transform 匹配的表会怎样？**

未匹配的表会继续在流水线中流动，可被使用正确 `plugin_input` 和 `table_match_regex`
的 Sink 捕获。

**Q: 能否在不停机的情况下向现有 CDC 流水线新增表？**

这取决于具体连接器。MySQL-CDC 在部分配置下支持动态发现新表，但为新表添加 Transform
仍需重启流水线。建议使用 `stop-with-savepoint` 将数据丢失降至最低（参见
[REST API v2 参考文档](../engines/zeta/rest-api-v2.md)）。

---

## 参考文档

- [TableMerge Transform 参考](table-merge.md)
- [TableRename Transform 参考](table-rename.md)
- [transform-multi-table 参考](transform-multi-table.md)
- [多表能力概览](../architecture/features/multi-table.md)
- [CDC 流水线架构](../architecture/cdc-pipeline-architecture.md)
- [REST API v2 参考文档](../engines/zeta/rest-api-v2.md)
