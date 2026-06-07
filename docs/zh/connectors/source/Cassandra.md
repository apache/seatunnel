import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra 源连接器

## 描述

从 Apache Cassandra 读取数据.

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

|       名称           |     类型      | 必需   | 默认值        |
|-------------------|-------------|------|---------------|
| host              | String      | 是    | -             |
| keyspace          | String      | 是    | -             |
| cql               | String      | 否 *  | -             |
| tables_configs    | List\<Map\> | 否 *  | -             |
| username          | String      | 否    | -             |
| password          | String      | 否    | -             |
| datacenter        | String      | 否    | datacenter1   |
| consistency_level | String      | 否    | LOCAL_ONE     |

> \* `cql` 与 `tables_configs` 二选一，必须提供其中之一。

### host [string]

`Cassandra` 的集群地址, 格式为 `host:port` , 允许指定多个 `hosts` . 例如
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

`Cassandra` 的键空间.

### cql [String]

查询 CQL，用于通过 Cassandra 会话读取单张表的数据。与 `tables_configs` 互斥。

### tables_configs [List\<Map\>]

多表读取配置，每个条目必须包含 `cql` 字段。与 `cql` 互斥。

示例条目：

```
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

## 示例

### 单表模式

```hocon
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
```

### 多表模式

```hocon
source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    tables_configs = [
      {
        cql = "SELECT id, name FROM test.table1"
      },
      {
        cql = "SELECT id, value FROM test.table2"
      }
    ]
  }
}
```

## 变更日志

<ChangeLog />
