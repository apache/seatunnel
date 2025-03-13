# Cassandra

> Cassandra 源连接器

## 描述

从 Apache Cassandra 读取数据.

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

|       名称           |  类型  | 必需 | 默认值 |
|-------------------|--------|----|---------------|
| host              | String | 是  | -             |
| keyspace          | String | 是  | -             |
| cql               | String | 是  | -             |
| username          | String | 否  | -             |
| password          | String | 否 | -             |
| datacenter        | String | 否 | datacenter1   |
| consistency_level | String | 否 | LOCAL_ONE     |

### host [string]

`Cassandra` 的集群地址, 格式为 `host:port` , 允许指定多个 `hosts` . 例如
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

`Cassandra` 的键空间.

### cql [String]

查询cql，用于通过Cassandra会话搜索数据.

### username [string]

`Cassandra` 用户的用户名.

### password [string]

`Cassandra` 用户的密码.

### datacenter [String]

`Cassandra` 数据中心, 默认为 `datacenter1`.

### consistency_level [String]

`Cassandra` 的写入一致性级别, 默认为 `LOCAL_ONE`.

## 示例

```hocon
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
```

## 变更日志

### 下一个版本

- 添加 Cassandra 源连接器

