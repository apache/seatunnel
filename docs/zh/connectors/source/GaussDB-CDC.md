import ChangeLog from '../changelog/connector-cdc-gaussdb.md';

# GaussDB CDC

> GaussDB CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

GaussDB CDC 连接器通过 PostgreSQL 兼容的逻辑复制协议读取 GaussDB 数据库的快照数据和增量数据。

当前实现复用 SeaTunnel PostgreSQL CDC 运行时。请使用 PostgreSQL 兼容形式的 JDBC URL，例如 `jdbc:postgresql://host:port/database`，并配置 Debezium PostgreSQL 支持的逻辑解码插件，例如 `pgoutput`。

当前连接器不解析 `mppdb_decoding` 二进制解码协议。

## 使用步骤

1. 在 GaussDB 实例上启用 logical WAL。

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. 确保 CDC 用户可以连接数据库，并具备逻辑复制所需权限。

3. 如果 update 和 delete 事件需要完整行数据，请将被采集表的 replica identity 设置为 `FULL`。

```sql
ALTER TABLE your_schema.your_table REPLICA IDENTITY FULL;
```

## 源端可选项

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | 字符串 | 是 | - | GaussDB 数据库 JDBC URL。请使用 PostgreSQL 兼容形式，例如 `jdbc:postgresql://localhost:5432/gaussdb_cdc?loggerLevel=OFF`。 |
| username | 字符串 | 是 | - | 连接数据库的用户名。 |
| password | 字符串 | 是 | - | 连接数据库的密码。 |
| database-names | 列表 | 否 | - | 需要监控的数据库名称。 |
| schema-name | 列表 | 否 | - | 需要监控的 Schema 名称。 |
| table-names | 列表 | 二选一 | - | 需要监控的表。请使用完整的 `database.schema.table` 格式，例如 `gaussdb_cdc.inventory.orders`。 |
| table-pattern | 字符串 | 二选一 | - | 需要监控的完整表名正则表达式。`table-names` 和 `table-pattern` 互斥。 |
| table-names-config | 列表 | 否 | - | 表级配置列表。无物理主键表可通过 `primaryKeys` 指定唯一键，也可通过 `snapshotSplitColumn` 指定快照拆分列。 |
| startup.mode | 枚举 | 否 | INITIAL | 启动模式。支持 `initial`、`snapshot-only`、`committed-offset`、`earliest` 和 `latest`。 |
| stop.mode | 枚举 | 否 | NEVER | 停止模式。当前支持值为 `never`。 |
| snapshot.split.size | 整型 | 否 | 8096 | 表快照的拆分大小，单位为行数。 |
| snapshot.fetch.size | 整型 | 否 | 1024 | 读取表快照时每次查询的最大条数。 |
| slot.name | 字符串 | 否 | seatunnel | 逻辑复制槽名称。同一个 GaussDB 实例上如果有多个 CDC 任务，请为每个任务配置不同的复制槽。 |
| decoding.plugin.name | 字符串 | 否 | pgoutput | 逻辑解码插件名称。支持值与 PostgreSQL CDC 连接器一致，例如 `pgoutput`、`decoderbufs`、`wal2json`。不支持 `mppdb_decoding`。 |
| server-time-zone | 字符串 | 否 | UTC | 数据库服务器会话时区。 |
| connect.timeout.ms | 时间间隔 | 否 | 30000 | 建立数据库连接的最大等待时间，单位为毫秒。 |
| connect.max-retries | 整型 | 否 | 3 | 建立数据库连接的最大重试次数。 |
| connection.pool.size | 整型 | 否 | 20 | JDBC 连接池大小。 |
| exactly_once | 布尔 | 否 | false | 在初始快照阶段启用精确一次语义。仅当 `startup.mode` 为 `initial` 时可用。 |
| format | 枚举 | 否 | DEFAULT | 输出格式。支持 `DEFAULT` 和 `COMPATIBLE_DEBEZIUM_JSON`。 |
| debezium | 配置 | 否 | - | 透传给内嵌 PostgreSQL CDC 引擎的 Debezium 配置。 |
| common-options |  | 否 | - | 源连接器通用参数，请参考 [Source Common Options](../common-options/source-common-options.md)。 |

## 任务示例

```hocon
env {
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  GaussDB-CDC {
    plugin_output = "customers_gaussdb_cdc"
    username = "gaussdb"
    password = "gaussdb_password"
    database-names = ["gaussdb_cdc"]
    schema-name = ["inventory"]
    table-names = ["gaussdb_cdc.inventory.orders"]
    url = "jdbc:postgresql://localhost:5432/gaussdb_cdc?loggerLevel=OFF"
    decoding.plugin.name = "pgoutput"
    slot.name = "seatunnel_gaussdb_cdc"
  }
}

sink {
  Console {
    plugin_input = "customers_gaussdb_cdc"
  }
}
```

## CDC 元数据字段

GaussDB CDC 暴露以下元数据字段，可配合 `Metadata` transform 使用：

| Field | Type | Description |
| --- | --- | --- |
| database | STRING | 源端数据库名称。 |
| table | STRING | 源端表名。 |
| rowKind | STRING | 变更类型，例如 insert、update 或 delete。 |
| ts_ms | LONG | 源端事件时间戳，单位为毫秒。 |
| delay | LONG | 事件时间与处理时间之间的延迟，单位为毫秒。 |

## Changelog

<ChangeLog />
