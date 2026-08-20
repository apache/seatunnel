import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra 接收器连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

以批处理方式将数据写入 Apache Cassandra。

sink 会把数据写入已经存在的 Cassandra 表。如果不配置 `fields`，连接器会使用目标 Cassandra 表里的全部字段。
如果配置了 `fields`，则只写这些字段，并且每个字段都必须存在于目标表中。

连接器不会自动创建 keyspace、表或缺失字段。启动任务前请先准备好目标 Cassandra 表结构。

## 支持的数据源信息

| 数据源      | 支持版本 | 依赖 |
|-----------|--------|------|
| Cassandra | 通用    | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cassandra) |

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 名称              | 类型    | 是否必填 | 默认值      | 描述 |
|-------------------|---------|----------|-------------|------|
| host              | String  | 是       | -           | Cassandra 集群地址，格式是 `host:port`，多个地址用逗号分隔。 |
| keyspace          | String  | 是       | -           | Cassandra 会话使用的 keyspace。 |
| table             | String  | 是       | -           | 目标 Cassandra 表名。 |
| username          | String  | 否       | -           | Cassandra 用户名，需要和 `password` 一起配置。 |
| password          | String  | 否       | -           | Cassandra 密码，需要和 `username` 一起配置。 |
| datacenter        | String  | 否       | datacenter1 | Cassandra Java Driver 使用的本地数据中心名称。 |
| consistency_level | String  | 否       | LOCAL_ONE   | 写入一致性级别，例如 `LOCAL_ONE`、`ONE`、`QUORUM`、`LOCAL_QUORUM`。 |
| fields            | Array   | 否       | -           | 要写入的目标字段。不配置时写入目标表的全部字段。 |
| batch_size        | int     | 否       | 5000        | 每次 flush 前最多缓存的行数。 |
| batch_type        | String  | 否       | UNLOGGED    | Cassandra batch 类型，常用值包括 `LOGGED`、`UNLOGGED`、`COUNTER`。 |
| async_write       | boolean | 否       | true        | 是否异步执行写入。 |
| common-options    |         | 否       | -           | Sink 插件通用参数，例如 `plugin_input`。 |

### host [string]

`Cassandra` 的集群地址，格式为 `host:port` , 允许指定多个 `hosts` . 例如
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

`Cassandra` 键空间.

### table [String]

`Cassandra` 的表名.

### username [string]

`Cassandra` 用户的用户名.

### password [string]

`Cassandra` 用户的密码.

### datacenter [String]

`Cassandra` 的数据中心, 默认为 `datacenter1`.

### consistency_level [String]

`Cassandra` 写入一致性级别, 默认为 `LOCAL_ONE`.

### fields [array]

需要写入到 `Cassandra` 的字段。如果不配置，连接器会读取目标表结构，并写入目标表中的全部字段。

如果配置了该选项，字段名必须存在于目标 Cassandra 表中，也必须存在于上游 SeaTunnel 数据中。

当上游数据中有不需要写入 Cassandra 的额外字段时，可以使用该选项只选择目标字段。

### batch_size [number]

通过 [Cassandra-Java-Driver](https://github.com/datastax/java-driver) 每次写入的行数,
默认值 `5000`.

### batch_type [String]

`Cassandra` 批处理模式, 默认值 `UNLOGGED`.

### async_write [boolean]

`cassandra` 是否以异步模式写入, 默认值 `true`.

### common-options

Sink 插件通用参数，详情请参考 [Sink 常用选项](../common-options/sink-common-options.md)。

## 注意事项

- 任务启动前，目标 keyspace 和表必须已经存在。
- `fields` 适合上游数据有额外字段、只想写入其中一部分字段的场景；它不是建表配置。
- `async_write = true` 可以提升吞吐，`batch_size` 用来控制每次 flush 前聚合的行数。
- 连接器底层使用 Cassandra Java Driver；认证和一致性相关参数与 Driver 保持一致，需要更强一致性时请结合
  集群的副本策略设置 `consistency_level`。
- `batch_type = "UNLOGGED"` 是常见默认值；当正确性优先于吞吐时使用 `LOGGED`，counter 表使用 `COUNTER`。

## 任务示例

### 写入 Cassandra

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
    cql = "select * from source_table"
    plugin_output = "source_table"
  }
}

sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "sink_table"
    async_write = true
  }
}
```

### 写入指定字段

```hocon
sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "sink_table"
    fields = ["id", "c_int", "c_text"]
    batch_size = 1000
    batch_type = "UNLOGGED"
    async_write = true
  }
}
```

### 将 MySQL CDC 事件流式写入 Cassandra

把 MySQL CDC 事件通过 Cassandra sink 写入下游，需要把 CDC 的行类型映射成表字段。Cassandra
按行执行 `INSERT`，CDC 的 `DELETE` 可以通过写入 `is_deleted` 字段并在下游过滤掉来实现：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql:3306/test"
    username = "root"
    password = "mysqlpw"
    table-names = ["test.orders"]
  }
}

sink {
  Cassandra {
    host = "cassandra1:9042,cassandra2:9042"
    keyspace = "test"
    table = "orders"
    fields = ["id", "order_id", "customer", "amount", "is_deleted"]
    consistency_level = "LOCAL_QUORUM"
    batch_size = 2000
    batch_type = "UNLOGGED"
    async_write = true
  }
}
```

> **注意**：上面示例中的 `is_deleted` 字段不会由 MySQL-CDC 自动产出，也不会被 Cassandra
> sink 根据 `RowKind` 推导。你需要自行提供——既可以让上游 MySQL 表本身带有 `is_deleted`
> 列，也可以在 source 和 sink 之间增加一个 Transform-V2（例如 `sql`、`replace`）从 CDC 的
> `RowKind` 合成该字段。否则 `DELETE` 事件会被当作普通 upsert 写回 Cassandra。

## 变更日志

<ChangeLog />
