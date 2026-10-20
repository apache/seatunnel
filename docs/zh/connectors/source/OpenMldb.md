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

查询直接读取在线表。集群读取不会提交离线 Spark 作业、返回作业元数据或修改 OpenMLDB 会话及全局执行模式。
此连接器不支持读取离线特征数据。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

多表模式下，`schema.fields` 声明每个查询返回的字段名称和类型，连接器在输出数据前校验查询结果。
SQL `NULL` 值保留为 `null`，包括数值和布尔类型，不会被转换为零或 `false`。

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
| sql             | string  | 条件必填 | -   | 单查询 SQL。与 `tables_configs` 必须二选一。 |
| tables_configs  | list    | 条件必填 | -   | 多表读取的查询及显式结果结构。 |
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

未配置 `tables_configs` 时，`sql` 必填，且不能为空字符串或仅包含空白字符。

该兼容模式使用 SDK 的输入结构接口获取表结构，结果列的数量、顺序和类型必须与输入结构一致。
需要列投影或别名时，请使用 `tables_configs` 并显式配置结果结构。

### tables_configs [list]

同一个 OpenMLDB 实例上的非空查询列表。每个配置项包含：

- `sql`：非空 SQL 查询。
- `database`：可选，用于覆盖必填的根级数据库配置。
- `schema.table`：唯一的输出表标识，用于下游路由。
- `schema.fields`：查询返回的全部字段名称及受支持的 SeaTunnel 类型。

字段名称必须与查询结果一致，区分大小写，可通过 SQL 别名进行匹配。
字段按名称映射，`schema.fields` 的顺序不必与 SQL 投影顺序一致。
结果列缺失、重复、多余或类型不匹配时，读取失败。

连接和超时选项必须配置在源级别。不得同时配置 `tables_configs` 与根级 `sql` 或 `schema`。
每个配置项可以使用不同的结果结构。

单个读取器依次执行所有查询。批模式下，所有查询成功后才报告完成，空结果不会跳过后续查询。
流模式下，每次轮询都会重新执行全部查询，因此可能产生重复记录；这不是 CDC 或增量轮询。
多表读取不增加并行度，也不提供跨表一致性快照或精确一次保证。

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

### 多表读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OpenMldb {
    cluster_mode = false
    host = "openmldb"
    port = 6527
    database = "shop"
    tables_configs = [
      {
        sql = "select id, amount from orders"
        schema {
          table = "shop.orders"
          fields {
            id = STRING
            amount = INT
          }
        }
      },
      {
        database = "crm"
        sql = "select id, name from customers"
        schema {
          table = "crm.customers"
          fields {
            id = STRING
            name = STRING
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
