import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> JDBC Phoenix Sink 连接器

## 描述

通过 [JDBC 连接器](Jdbc.md) 将数据写入 Phoenix。Phoenix 写入通常使用 `UPSERT` 语句，并通过 Phoenix JDBC 驱动落到对应的 HBase 表。

Phoenix 可以通过两种 JDBC 方式连接：一种是 thick 驱动连接 ZooKeeper，另一种是 thin 驱动连接 Phoenix Query Server。

> 默认情况下，JDBC 连接器模块使用 Phoenix thin 驱动。如果需要 thick 驱动或其他版本的 Phoenix thin 驱动，需要使用对应驱动重新编译 JDBC 连接器模块。
>
> Phoenix Sink 不支持精确一次语义，因为当前 Phoenix JDBC 写入路径不支持 XA 事务。

## 主要特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| driver | String | 是 | - | Phoenix JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用 `org.apache.phoenix.queryserver.client.Driver`。 |
| url | String | 是 | - | Phoenix JDBC URL。thick 驱动示例：`jdbc:phoenix:localhost:2182/hbase`；thin 驱动示例：`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。 |
| query | String | 是 | - | 写入 Phoenix 的 SQL。Phoenix 通常使用 `UPSERT INTO ... VALUES (?, ?)`，也可以使用 JDBC Sink 支持的命名参数。 |
| batch_size | Int | 否 | 1000 | 当缓存数据行数达到该值时刷新写入。 |
| batch_interval_ms | Long | 否 | 1000 | 即使未达到 `batch_size`，达到该时间间隔后也会刷新写入。 |
| common-options | | 否 | - | Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。 |

Phoenix Sink 基于共享 JDBC Sink 实现，因此 `max_retries`、`properties`、`field_ide`、`auto_commit` 等高级 JDBC 写入参数遵循 [JDBC Sink](Jdbc.md) 的规则。不要为 Phoenix 开启 `is_exactly_once`，因为 Phoenix JDBC 写入路径不支持 XA 事务。

## 示例

### Thick 驱动

```hocon
sink {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "upsert into test.SINK(age, name) values(?, ?)"
  }
}
```

### Thin 驱动

```hocon
sink {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://seatunnel_e2e_phoenix:8765;serialization=PROTOBUF"
    query = "upsert into test.SINK(age, name) values(?, ?)"
    batch_size = 1000
    batch_interval_ms = 2000
  }
}
```

## 变更日志

<ChangeLog />
