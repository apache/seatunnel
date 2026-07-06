import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> JDBC Phoenix 源连接器

## 描述

通过 [JDBC 连接器](Jdbc.md) 读取 Phoenix 数据。Phoenix 可以通过两种 JDBC 方式连接：一种是 thick 驱动连接 ZooKeeper，另一种是 thin 驱动连接 Phoenix Query Server。

该连接器支持批处理任务。流任务中可以把 Phoenix 作为有界 JDBC Source 使用，但 Phoenix Source 不会持续捕获新增变更。

> 默认情况下，JDBC 连接器模块使用 Phoenix thin 驱动。如果需要 thick 驱动或其他版本的 Phoenix thin 驱动，需要使用对应驱动重新编译 JDBC 连接器模块。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持查询SQL，可以实现投影效果.

- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| driver | String | 是 | - | Phoenix JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用 `org.apache.phoenix.queryserver.client.Driver`。 |
| url | String | 是 | - | Phoenix JDBC URL。thick 驱动示例：`jdbc:phoenix:localhost:2182/hbase`；thin 驱动示例：`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。 |
| query | String | 是 | - | 从 Phoenix 读取数据的 SQL，可用于选择需要读取的列和行。 |
| common-options | | 否 | - | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。 |

Phoenix Source 基于共享 JDBC Source 实现，因此 `fetch_size`、`partition_column`、`partition_num`、`properties`、`table_list` 等高级 JDBC 读取参数遵循 [JDBC Source](Jdbc.md) 的规则。

## 示例

### Thick 驱动

```hocon
source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select age, name from test.SOURCE"
  }
}
```

### Thin 驱动

```hocon
source {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://seatunnel_e2e_phoenix:8765;serialization=PROTOBUF"
    query = "select age, name from test.SOURCE"
  }
}
```

## 变更日志

<ChangeLog />
