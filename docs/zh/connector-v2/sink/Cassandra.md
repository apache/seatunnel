# Cassandra

> Cassandra 接收器连接器

## 描述

将数据写入 Apache Cassandra.

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|       名称           |  类型  | 必需 | 默认值 |
|-------------------|---------|----|---------------|
| host              | String  | 是  | -             |
| keyspace          | String  | 是  | -             |
| table             | String  | 是  | -             |
| username          | String  | 否  | -             |
| password          | String  | 否 | -             |
| datacenter        | String  | 否 | datacenter1   |
| consistency_level | String  | 否 | LOCAL_ONE     |
| fields            | Array   | 否 | -             |
| batch_size        | int     | 否 | 5000          |
| batch_type        | String  | 否 | UNLOGGED      |
| async_write       | boolean | 否 | true          |

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

需要输出到 `Cassandra` 的数据字段, 如果未配置, 如果未配置，它将自动适应 sink 表 `schema`.

### batch_size [number]

通过 [Cassandra-Java-Driver](https://github.com/datastax/java-driver) 每次写入的行数,
默认值 `5000`.

### batch_type [String]

`Cassandra` 批处理模式, 默认值 `UNLOGGER`.

### async_write [boolean]

`cassandra` 是否以异步模式写入, 默认值 `true`.

## 示例

```hocon
sink {
 Cassandra {
     host = "localhost:9042"
     username = "cassandra"
     password = "cassandra"
     datacenter = "datacenter1"
     keyspace = "test"
    }
}
```

## 变更日志

### 下一个版本

- 添加 Cassandra 接收器连接器

