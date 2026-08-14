import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> Phoenix 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 [Jdbc 连接器](Jdbc.md) 从 Apache Phoenix 读取数据。作业配置中使用的连接器标识符为 `Jdbc`。
已测试的 Phoenix 版本为 4.x 和 5.x。

底层通过 Phoenix 的 JDBC 驱动执行查询语句并从 HBase 读取行数据。支持标准 `SELECT ...` 语法的
列投影。

使用 Java JDBC 连接 Phoenix 有两种方式：

- 通过 **thick** 驱动连接 ZooKeeper 集群；
- 通过 **thin** 驱动连接 Phoenix Query Server。

> **提示：** 默认使用（thin）驱动 jar。如果需要使用（thick）驱动或者其他版本的 Phoenix（thin）
> 驱动，需要重新编译 `connector-jdbc` 模块。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持标准 SQL 查询，可通过投影列裁剪输出字段。

- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 是否必填 | 默认值 | 描述                                                                                            |
|----------------|--------|----------|--------|-------------------------------------------------------------------------------------------------|
| driver         | String | 是       | -      | JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用 `org.apache.phoenix.queryserver.client.Driver`。 |
| url            | String | 是       | -      | JDBC 连接 URL。thick 驱动使用 `jdbc:phoenix:localhost:2182/hbase`，thin 驱动使用 `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。 |
| query          | String | 是       | -      | 读取数据时执行的 SELECT 查询，例如 `select age, name from test.source`。SELECT 列表的列顺序需要和 `schema.fields` 一致。 |
| common-options |        | 否       | -      | 源插件通用参数，详见 [Source 通用选项](../common-options/source-common-options.md)。           |

### driver [string]

JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用
`org.apache.phoenix.queryserver.client.Driver`。

### url [string]

JDBC 连接 URL。thick 驱动使用 `jdbc:phoenix:localhost:2182/hbase`，thin 驱动使用
`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。

### query [string]

读取数据时执行的 SELECT 查询语句。表名要使用带 schema 的完全限定名（例如 `test.source`），不能只
写表名。SELECT 列表中按需列出要读取的列即可使用 Phoenix 的列投影能力。

### common options

源插件通用参数，详见 [Source 通用选项](../common-options/source-common-options.md)。

## 任务示例

### 使用 thick 驱动

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select age, name from test.source"
  }
}

sink {
  Console {}
}
```

### 使用 thin 驱动

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "select age, name from test.source"
  }
}

sink {
  Console {}
}
```

### 列投影并按条件过滤

可以结合列投影和 `WHERE` 条件提前把不需要的行过滤掉。下面的示例只读取 `name` 以 `A`
开头的行：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select name, score from test.source where name like 'A%'"
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />