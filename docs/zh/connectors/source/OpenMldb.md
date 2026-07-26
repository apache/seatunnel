import ChangeLog from '../changelog/connector-openmldb.md';

# OpenMldb

> OpenMldb 源连接器

## 描述

用于从 OpenMldb 读取数据.

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

|      名称       | 类型    | 必需 | 默认值 | 描述 |
|-----------------|---------|------|--------|------|
| cluster_mode    | boolean | 是   | -      | 是否以 OpenMLDB 集群模式连接。 |
| sql             | string  | 是   | -      | 用于读取数据的 SQL 语句。 |
| database        | string  | 是   | -      | 数据库名称。 |
| host            | string  | 否   | -      | 当 `cluster_mode` 为 `false` 时必填。 |
| port            | int     | 否   | -      | 当 `cluster_mode` 为 `false` 时必填。 |
| zk_host         | string  | 否   | -      | 当 `cluster_mode` 为 `true` 时必填。 |
| zk_path         | string  | 否   | -      | 当 `cluster_mode` 为 `true` 时必填。 |
| session_timeout | int     | 否   | 10000  | OpenMLDB 会话超时时间，单位毫秒。 |
| request_timeout | int     | 否   | 60000  | OpenMLDB 请求超时时间，单位毫秒。 |
| common-options  |         | 否   | -      | 源插件通用参数。 |

### cluster_mode [boolean]

是否以 OpenMLDB 集群模式连接。为 `false` 时配置 `host` 和 `port`；为 `true` 时配置 `zk_host` 和 `zk_path`。

### sql [string]

Sql 语句

### database [string]

数据库名称

### host [string]

OpenMldb主机，仅支持OpenMldb单模

### port [int]

OpenMldb端口，仅支持OpenMldb单模

### zk_host [string]

Zookeeper主机，仅在OpenMldb集群模式下受支持

### zk_path [string]

Zookeeper路径，仅在OpenMldb集群模式下受支持

### session_timeout [int]

OpenMLDB 会话超时时间，单位毫秒。

### request_timeout [int]

OpenMLDB 请求超时时间，单位毫秒。

### common options

源插件常用参数, 详见 [Source Common Options](../common-options/source-common-options.md) 

## 示例

```hocon
source {
  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }
}
```

集群模式示例：

```hocon
source {
  OpenMldb {
    zk_host = "zk-1:2181,zk-2:2181,zk-3:2181"
    zk_path = "/openmldb"
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = true
  }
}
```

## 变更日志

<ChangeLog />
