import ChangeLog from '../changelog/connector-openmldb.md';

# OpenMldb

> OpenMldb 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 OpenMLDB 读取数据。连接器会执行配置的 SQL 语句并把结果转换为 SeaTunnel 记录，同时支持
单机版和集群版两种部署模式。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

OpenMLDB 类型会按照所配置 `sql` 语句的结果集映射为 SeaTunnel 类型。SeaTunnel 不原生支持的类型会直接
导致读取失败，并抛出 `UNSUPPORTED_DATA_TYPE` 错误。

| OpenMLDB 数据类型 | SeaTunnel 数据类型 |
|-------------------|--------------------|
| bool              | boolean            |
| smallint | smallint |
| int       | int      |
| bigint    | bigint   |
| float / double    | float / double     |
| string / varchar  | string             |
| date              | date               |
| timestamp         | timestamp          |

## 选项

|      名称       |  类型   | 必需 | 默认值 | 描述                                                                                              |
|-----------------|---------|------|--------|---------------------------------------------------------------------------------------------------|
| cluster_mode    | boolean | 是   | -      | 是否以 OpenMLDB 集群模式连接。`false` 表示单机模式，`true` 表示集群模式。                          |
| sql             | string  | 是   | -      | 用于读取数据的 SQL 语句，列名和类型按结果集定义。                                                  |
| database        | string  | 是   | -      | 要连接的 OpenMLDB 数据库名称。                                                                    |
| host            | string  | 否   | -      | 当 `cluster_mode` 为 `false` 时必填，OpenMLDB 单机版主机地址。                                     |
| port            | int     | 否   | -      | 当 `cluster_mode` 为 `false` 时必填，OpenMLDB 单机版端口。                                         |
| zk_host         | string  | 否   | -      | 当 `cluster_mode` 为 `true` 时必填，OpenMLDB 集群对应的 ZooKeeper 地址列表。                       |
| zk_path         | string  | 否   | -      | 当 `cluster_mode` 为 `true` 时必填，OpenMLDB 集群在 ZooKeeper 上的路径，例如 `/openmldb`。        |
| session_timeout | int     | 否   | 10000  | OpenMLDB 会话超时时间，单位毫秒。                                                                 |
| request_timeout | int     | 否   | 60000  | OpenMLDB 请求超时时间，单位毫秒。                                                                 |
| common-options  |         | 否   | -      | 源插件通用参数，详见 [Source 常见选项](../common-options/source-common-options.md)。              |

### cluster_mode [boolean]

是否以 OpenMLDB 集群模式连接。为 `false` 时配置 `host` 和 `port`；为 `true` 时配置 `zk_host`
和 `zk_path`。

### sql [string]

针对 OpenMLDB 执行的 SQL 语句，结果集的列会成为连接器输出行的字段。

### database [string]

要连接的 OpenMLDB 数据库名称，配置的数据库必须在目标 OpenMLDB 实例上存在。

### host [string]

OpenMLDB 主机，仅在 `cluster_mode` 为 `false`（单机模式）下使用。

### port [int]

OpenMLDB 端口，仅在 `cluster_mode` 为 `false`（单机模式）下使用。

### zk_host [string]

OpenMLDB 集群对应的 ZooKeeper 地址列表，例如 `zk-1:2181,zk-2:2181,zk-3:2181`，仅在
`cluster_mode` 为 `true` 时使用。

### zk_path [string]

OpenMLDB 集群在 ZooKeeper 上的路径，例如 `/openmldb`，仅在 `cluster_mode` 为 `true` 时使用。

### session_timeout [int]

OpenMLDB 会话超时时间，单位毫秒，默认 `10000`（10 秒）。

### request_timeout [int]

OpenMLDB 请求超时时间，单位毫秒，默认 `60000`（60 秒）。

### common options

源插件通用参数，详见 [Source 常见选项](../common-options/source-common-options.md)。

## 任务示例

### 单机模式

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

### 集群模式

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

### 配合下游接收器

从 OpenMLDB 读取数据并通过 Console 接收器打印的典型端到端作业。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select id, name from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />
