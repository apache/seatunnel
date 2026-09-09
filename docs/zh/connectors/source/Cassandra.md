import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra 源连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

以批处理方式从 Apache Cassandra 读取数据。

Cassandra source 支持两种读取方式：

- 使用 `cql` 读取单张表。
- 使用 `tables_configs` 读取多张表，每个条目里配置一个 `cql`。

连接器会根据 CQL 返回结果里的列名和数据类型生成下游数据结构，所以 CQL 应该返回下游真正需要的列。

## 支持的数据源信息

| 数据源      | 支持版本 | 依赖 |
|-----------|--------|------|
| Cassandra | 通用    | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cassandra) |

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| Cassandra 数据类型 | SeaTunnel 数据类型 |
|-------------------|--------------------|
| ascii             | STRING             |
| varchar/text      | STRING             |
| varint            | STRING             |
| uuid/timeuuid     | STRING             |
| inet              | STRING             |
| tinyint           | BYTE               |
| smallint          | SHORT              |
| int               | INT                |
| bigint/counter    | LONG               |
| float             | FLOAT              |
| double/decimal    | DOUBLE             |
| boolean           | BOOLEAN            |
| time              | TIME               |
| date              | DATE               |
| timestamp         | TIMESTAMP          |
| blob              | ARRAY\<BYTE\>      |
| list              | ARRAY              |
| set               | ARRAY              |
| map               | MAP                |

## Source 选项

| 名称              | 类型       | 是否必填 | 默认值      | 描述 |
|-------------------|------------|----------|-------------|------|
| host              | String     | 是       | -           | Cassandra 集群地址，格式是 `host:port`，多个地址用逗号分隔。 |
| keyspace          | String     | 是       | -           | Cassandra 会话使用的 keyspace。 |
| cql               | String     | 否 *     | -           | 读取单张表时使用的 CQL。 |
| tables_configs    | List\<Map\> | 否 *     | -           | 多表读取配置，每个条目必须包含一个 `cql`。 |
| username          | String     | 否       | -           | Cassandra 用户名，需要和 `password` 一起配置。 |
| password          | String     | 否       | -           | Cassandra 密码，需要和 `username` 一起配置。 |
| datacenter        | String     | 否       | datacenter1 | Cassandra Java Driver 使用的本地数据中心名称。 |
| consistency_level | String     | 否       | LOCAL_ONE   | 读取一致性级别，例如 `LOCAL_ONE`、`ONE`、`QUORUM`、`LOCAL_QUORUM`。 |
| common-options    |            | 否       | -           | Source 插件通用参数，例如 `plugin_output`。 |

> \* `cql` 与 `tables_configs` 二选一，必须提供其中之一。

### host [string]

`Cassandra` 的集群地址, 格式为 `host:port` , 允许指定多个 `hosts` . 例如
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

`Cassandra` 的键空间.

### cql [String]

查询 CQL，用于读取单张表的数据。它和 `tables_configs` 互斥。

连接器会使用 CQL 返回结果里的元数据来生成输出结构。通常建议写成能返回真实表字段的查询，例如
`select * from source_table` 或 `select id, name from source_table`。

### tables_configs [List\<Map\>]

多表读取配置，每个条目必须包含 `cql` 字段。它和根层级的 `cql` 互斥。

不要在 `tables_configs` 中重复配置同一张源表，连接器启动时会检查重复表名。

示例条目：

```hocon
{
  cql = "SELECT id, name FROM keyspace.table1"
}
```

### username [string]

`Cassandra` 用户的用户名.

### password [string]

`Cassandra` 用户的密码.

### datacenter [String]

`Cassandra` 数据中心, 默认为 `datacenter1`.

### consistency_level [String]

`Cassandra` 的读取一致性级别, 默认为 `LOCAL_ONE`.

### common-options

Source 插件通用参数，详情请参考 [Source 常用选项](../common-options/source-common-options.md)。

## 注意事项

- `username` 和 `password` 是一组配置。集群开启认证时两个都要配；未开启认证时两个都可以不配。
- `datacenter` 必须和 Cassandra 集群的本地数据中心名称一致。默认值是 `datacenter1`，这也是常见
  Testcontainers 环境里的默认值。
- `cql` 和 `tables_configs` 互斥。读取一个结果表时用 `cql`，需要让一个 source 读取多张 Cassandra 表时用
  `tables_configs`。
- 这是批处理 source。它读取当前查询结果后就会结束。
- 一个 CQL 查询会作为一个 source split 读取。调大任务并行度不会自动把单张 Cassandra 表拆成多个扫描任务。
- 连接器底层使用 Cassandra Java Driver，本文档列出的连接选项是连接器实际读取的全部设置；其他
  DataStax Driver 选项沿用其内置默认值。

## 任务示例

### 单表读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    cql = "SELECT * FROM test.source_table"
    plugin_output = "source_table"
  }
}

sink {
  Console {}
}
```

### 多表读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    tables_configs = [
      {
        cql = "select id, c_int from mt_source_a"
      },
      {
        cql = "select id, c_int from mt_source_b"
      }
    ]
  }
}

sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "mt_sink_table"
  }
}
```

### 提高读取一致性级别

当读取结果必须满足配置的副本因子时，使用 `consistency_level = "QUORUM"`，并配合
`datacenter` 让 Driver 连接到正确的本地协调节点：

```hocon
source {
  Cassandra {
    host = "cassandra1:9042,cassandra2:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    consistency_level = "QUORUM"
    cql = "SELECT id, name, score FROM test.accounts"
  }
}
```

## 变更日志

<ChangeLog />
