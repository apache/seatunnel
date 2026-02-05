---
sidebar_position: 4
title: CatalogTable 和元数据管理
---

# CatalogTable 和元数据管理

## 1. 概述

### 1.1 问题背景

数据集成需要显式的模式管理:

- **模式定义**: 如何定义和验证表模式?
- **模式传播**: 如何在数据源(source) → 转换器(transform) → 目标端(sink)之间传递模式?
- **模式演化**: 如何处理运行时 DDL 变更(添加/删除列)?
- **类型映射**: 如何在不同数据源之间映射类型?
- **元数据完整性**: 如何捕获完整的表元数据(约束、分区)?

### 1.2 设计目标

SeaTunnel 的元数据管理旨在:

1. **类型安全**: 在作业提交时进行显式模式验证
2. **完整性**: 捕获所有表元数据(列、约束、分区、选项)
3. **支持演化**: 处理运行时模式变更(DDL 同步)
4. **引擎独立**: 模式表示独立于执行引擎
5. **易用性**: 用于模式创建和转换的简单 API

## 2. 核心概念

### 2.1 CatalogTable

包含所有元数据的表的完整表示。

CatalogTable 是 SeaTunnel 对“表及其元数据”的统一表示，通常包含:
- **tableId**: 表标识(可定位到 catalog/database/schema/table)
- **tableSchema**: 模式定义(列、主键、约束等)
- **options**: 连接器/表级选项(如实际表名、topic、format 等)
- **partitionKeys**: 分区键(可选)
- **comment/catalogName**: 注释与归属 catalog 信息(可选)

**关键组件**:
- `TableIdentifier`: 唯一表标识(catalog.database[.schema].table)
- `TableSchema`: 包含列、主键、约束的模式
- `options`: 连接器特定设置(例如 Kafka 主题、JDBC 表名)
- `partitionKeys`: 分区表的分区列

### 2.2 TableSchema

包含列和约束的模式定义。

TableSchema 关注“表有哪些列，以及这些列有哪些约束”:
- **columns**: 列定义列表(顺序敏感)
- **primaryKey**: 主键定义(可选)
- **constraintKeys**: 唯一键/外键等约束(可选)

### 2.3 Column

包含类型和约束的列定义。

Column 通常由以下信息构成:
- **name**: 列名
- **dataType**: SeaTunnelDataType 统一类型
- **nullable/defaultValue**: 空值与默认值语义
- **comment/options**: 备注与连接器/列级扩展选项

### 2.4 SeaTunnelDataType

跨连接器的统一类型系统。

**基本类型**(示例):
- 数值: TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/DECIMAL(precision, scale)
- 字符串: STRING/CHAR(length)/VARCHAR(length)
- 二进制: BYTES
- 日期/时间: DATE/TIME/TIMESTAMP
- 布尔: BOOLEAN

**复杂类型**(示例):
- ARRAY(elementType)
- MAP(keyType, valueType)
- ROW(fields)

## 3. 模式创建

### 3.1 构建器模式

推荐的构建步骤:
1. 明确 TableIdentifier(作业内唯一定位)
2. 通过 TableSchema.Builder 按顺序定义 columns
3. 若需要去重/更新语义，定义 primaryKey
4. 写入 options(连接器侧的物理映射信息)
5. 如为分区表，补充分区键 partitionKeys

### 3.2 列构建器

列定义需要尽量显式:
- name/dataType 是必选
- nullable/defaultValue 决定写入与 DDL 的语义
- comment/options 用于补充连接器侧能力(例如精度、编码、额外属性)

### 3.3 主键和约束

约束表达要点:
- primaryKey/uniqueKey 是“语义约束”，用于:
  - 转换/下游写入侧的幂等键选择
  - schema 兼容性校验
  - 部分连接器的 DDL 自动生成
- 外键等约束在跨系统同步时常受限于目标端能力与时序一致性，通常需要在“可用性/一致性”之间做权衡

## 4. 模式传播

### 4.1 数据源 → 转换器 → 目标端流程

```
┌──────────────┐
│数据源(source) │
│              │
│  生产         │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (输入模式)
┌──────────────┐
│   转换器      │
│              │
│  修改         │
│ CatalogTable │
└──────┬───────┘
       │
       ▼ (输出模式)
┌──────────────┐
│   目标端      │
│              │
│  验证         │
│ CatalogTable │
└──────────────┘
```

### 4.2 数据源模式生产

数据 Source 读取端的职责:
- 从外部系统读取元数据(列、类型、主键/唯一键、分区、注释等)
- 将外部类型映射为 SeaTunnelDataType
- 产出 CatalogTable，作为作业的“输入契约”

常见失败模式:
- 元数据读取失败(权限/网络/超时)
- 类型无法映射(外部类型超出 SeaTunnel 统一类型系统)
- schema 漂移(运行中 DDL)导致“生产的 CatalogTable”与真实数据不一致

### 4.3 转换器模式转换

转换器端的职责:
- 根据转换逻辑(表达式/字段选择/重命名等)计算输出 schema
- 保证输出 CatalogTable 可被下游 sink 验证与消费

常见风险:
- schema 推断不精确(例如 UDF、动态字段)
- 类型提升/缩窄导致的精度或溢出问题
- 字段重命名/删除导致下游找不到列

### 4.4 目标端模式验证

目标端侧的职责:
- 获取输入 CatalogTable(来自上游)
- 获取目标端的真实表/索引元数据(或根据配置选择 auto-create)
- 做兼容性校验:
  - 列是否存在/是否允许自动新增
  - 类型是否兼容(是否允许安全扩展)
  - 约束/主键是否满足写入语义(尤其是 upsert/exactly-once)

推荐策略:
- 早期失败：在作业启动阶段就完成校验，避免运行中才暴露不可写入
- 明确兼容规则：哪些类型扩展允许、哪些缩窄禁止、如何处理 nullability 变化

## 5. 模式演化

### 5.1 SchemaChangeEvent

SchemaChangeEvent 表示 **CDC 数据源捕获到的 DDL/元数据变更**，用于在数据流中传递“表结构发生了什么变化”。

核心语义:
- 变更必须能定位到具体表（TableIdentifier/TablePath 等）
- 变更类型是可枚举的（如新增列、删除列、修改列、重命名、主键/约束变化等）
- 变更负载以“语义化描述”为主（列名、类型、nullable、默认值等），而不是下游可直接执行的 SQL

为什么要事件化:
- 对上游 CDC 而言，结构变化是数据的一部分，必须被可靠传播
- 对下游（Transform/Sink）而言，结构变化通常需要与“业务兼容性规则”共同决策（允许/禁止、自动/人工）

失败模式与建议:
- 事件丢失：下游 schema 与数据不一致，建议将 schema 事件纳入 checkpoint/恢复语义（至少保证“数据与变更事件的相对顺序”可恢复）
- 顺序错乱：先收到数据后收到 DDL，建议在 Source 侧保证同一表内顺序一致，或在下游做缓冲与重放
- 不可应用变更：例如删除列/缩窄类型导致不可写，建议启动阶段明确策略并在运行时可观测告警

### 5.2 CDC 数据源模式演化

CDC Source 的职责不是“执行 DDL”，而是 **把变更识别出来并以事件形式注入数据流**。

推荐工作流:
1. 捕获上游变更（binlog/redo log/DDL log/元数据快照差异）
2. 解析为结构化事件（新增/删除/修改列等）
3. 与数据事件一同向下游发出，保证同一表内的顺序可解释
4. 在 checkpoint/恢复时保证：不会出现“数据前进但 schema 事件回退”的不可恢复状态

常见边界:
- DDL 批量发生：可能产生多个事件，应明确合并/拆分规则与顺序
- 同名列重复/大小写规则：需与 Catalog/TableIdentifier 规范对齐
- DDL 解析失败：建议降级为“停止作业 + 明确报错”，或按配置选择“跳过变更 + 记录告警”（默认不推荐）

### 5.3 转换器模式演化映射

Transform 侧需要回答的问题是：**上游 schema 变化，在经过转换逻辑后，等价的下游变化是什么？**

典型规则:
- 字段选择：如果下游不再保留该列，则“新增列事件”可被忽略；但“删除列事件”可能仍需要传播以便下游校验
- 字段重命名：需要把事件中的列名同步映射
- 类型转换：需要把“上游类型变化”映射为“下游类型变化”（例如 cast、精度变化）
- 表达式生成列：上游新增列不一定影响下游，但下游可能新增派生列（属于转换器内部 schema 变化）

失败模式:
- 无法判定影响：例如 UDF 返回动态字段，建议显式配置输出 schema 或选择“禁止自动演化”
- 不可逆转换：例如精度缩窄/字符串解析失败，建议在演化阶段就拒绝或要求人工介入

### 5.4 目标端模式演化应用

Sink 侧的职责是 **对变更做兼容性决策并落地到目标系统**（如果启用自动演化）。

推荐处理流程:
1. 获取目标端当前表/索引元数据（可能来自 Catalog、JDBC 元数据、Hive Metastore 等）
2. 按策略判断是否允许该类变更（如自动建表、自动新增列、是否允许 drop/rename）
3. 将“语义事件”转换成目标系统的 DDL/元数据 API 调用
4. 将变更落地动作纳入可恢复语义：
   - 如果 sink 支持 2PC/事务，则尽量在 commit 阶段与数据提交协同
   - 如果目标端 DDL 不能事务化，至少保证幂等与可重试（例如“列已存在”视为成功）

失败模式与建议:
- DDL 执行失败：目标端权限/锁冲突/存储限制，建议快速失败并输出明确告警，避免 silent skip
- 并发变更：多个并行 writer 同时尝试演化，建议统一到单点/串行执行（或使用外部锁）
- 演化与写入竞争：写入在 DDL 未生效时到达，建议在应用变更后再放行数据，或使用缓冲/重试

## 6. 类型映射

### 6.1 JDBC 类型映射

JDBC 类型映射的目标是把“目标系统类型”规范化为 SeaTunnel 内部类型（SeaTunnelDataType），从而让上游/下游对齐 schema 语义。

映射原则:
- 尽量保持语义而非字面：例如 `VARCHAR`/`LONGVARCHAR` 最终都可能落到 `STRING`
- 保留关键约束：长度、精度、scale、时区（如果目标系统支持）
- 明确不可映射类型的策略：快速失败 vs 降级为 `STRING/BYTES`（默认建议失败）

兼容性与风险:
- 精度相关：`DECIMAL(p,s)` 的 `p/s` 需要完整保留，否则可能出现截断/溢出
- 时间相关：`TIMESTAMP`/`TIMESTAMP WITH TIME ZONE` 的语义差异需要明确
- 二进制相关：`BINARY/VARBINARY` 建议映射为 `BYTES`，不要静默转字符串

### 6.2 Kafka (Avro) 类型映射

Avro/Protobuf/JSON Schema 等“消息协议”通常是嵌套结构，映射时需要同时处理:
- 基础类型：int/long/string/bytes/bool 等
- 复合类型：array/map/record（对应 SeaTunnel 的 ARRAY/MAP/ROW）
- 兼容性规则：新增字段、字段默认值、union/nullability

推荐策略:
- 将 `record` 映射为 `ROW`，并保持字段顺序与名字稳定
- 对 nullable：显式表达（而不是隐式 union）
- 对 schema registry：把 schema 版本作为可观测信息输出，便于排障与回滚

## 7. 分区表

### 7.1 分区定义

分区信息是 CatalogTable 的一部分：它把“表 schema”与“物理分布/组织方式”连接起来。

分区键的典型用途:
- 让 Source 能按分区裁剪（partition pruning），减少扫描范围
- 让 Sink 能按分区写入，提高写入性能并避免热点
- 让下游表管理系统（Hive/Iceberg/Hudi）正确理解数据布局

### 7.2 分区感知数据源

Source 侧的关键是：从外部元数据系统读取“分区键定义”并写入 Produced CatalogTable。

推荐能力:
- 支持分区过滤条件（按时间/范围），并明确过滤是在“枚举 split”阶段完成
- 分区元数据缺失时快速失败，避免静默全表扫描

### 7.3 分区感知目标端

Sink 侧的关键是：把输入行映射到正确分区并以目标系统要求的方式提交。

常见失败模式:
- 分区键缺失/为空：需要明确处理策略（拒绝、写入默认分区、或降级为非分区写入）
- 分区字段类型不匹配：建议在启动阶段做 schema 校验
- 并发写入同分区：需要考虑文件/小文件合并、提交冲突与幂等

## 8. 最佳实践

### 8.1 模式定义

**优先使用显式模式**:
- 推荐：在配置或作业定义阶段显式给出 schema（字段名、类型、nullable、精度等）
- 不推荐：完全依赖运行时推断（尤其是“取第一行推断”），容易在脏数据或字段漂移时产生不可恢复的问题

**选择合适类型**:
- 推荐：金额/计数等使用 `DECIMAL(p,s)`/`BIGINT` 等精确类型；时间使用 `DATE/TIME/TIMESTAMP`
- 不推荐：将所有字段降级为 `STRING`，会把错误推迟到下游并放大数据质量成本

### 8.2 模式验证

**早期验证**（快速失败）:
- Source：在 open/prepare 阶段确定 Produced CatalogTable，并完成“字段存在性/类型合法性/可投影性”等验证
- Sink：在作业启动阶段完成“输入 schema 与目标表 schema”的兼容性校验，避免运行中才暴露不可写入

### 8.3 类型兼容性

**类型扩展（通常安全）**:
- `INT → BIGINT`
- `FLOAT → DOUBLE`
- `VARCHAR(10) → VARCHAR(20)`

**类型缩窄（通常不安全）**:
- `BIGINT → INT`（溢出风险）
- `DOUBLE → FLOAT`（精度损失）
- `VARCHAR(20) → VARCHAR(10)`（截断风险）

## 9. 配置

### 9.1 模式覆盖

```hocon
source {
  Jdbc {
    url = "..."
    query = "SELECT * FROM users"

    # 覆盖推断的模式
    schema {
      fields {
        id = "BIGINT"
        name = "STRING"
        age = "INT"
      }
    }
  }
}
```

### 9.2 模式演化控制

在 **CDC 场景**下，SeaTunnel 的模式演化通常由 **CDC Source 侧开关**控制：在 CDC 源启用 `schema-changes.enabled = true` 后，运行时 DDL/元数据变更会随数据流传播；下游 Sink 是否能自动应用变更取决于连接器是否支持 schema evolution。

下面给出一个“CDC → JDBC Sink”的最小可用示例（参数以各连接器文档为准）：

```hocon
source {
  MySQL-CDC {
    url = "..."
    table-names = ["db.table"]

    # 启用 CDC 模式变更事件（SchemaChangeEvent）传播
    schema-changes.enabled = true
  }
}

sink {
  Jdbc {
    url = "..."

    # 让 JDBC sink 能根据上游 schema 生成/刷新写入 SQL
    generate_sink_sql = true

    # 作业启动阶段：若表不存在则创建（用于首次建表）
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  }
}
```

> 说明：当前仓库中没有“schema-evolution 统一配置块”这一通用写法。
> 新增/删除/重命名列等是否自动应用由具体 Sink 实现与目标端能力决定；其中 DROP/RENAME 属于高风险操作，建议在生产环境谨慎启用并做好灰度与回滚预案。

## 10. 相关资源

- [source 数据源架构](source-architecture.md)
- [sink 目标端架构](sink-architecture.md)
- [模式演化](../../introduction/concepts/schema-evolution.md)
- [模式特性](../../introduction/concepts/schema-feature.md)
