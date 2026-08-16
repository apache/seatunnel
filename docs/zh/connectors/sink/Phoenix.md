import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> Phoenix 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 [Jdbc 连接器](Jdbc.md) 把数据写入 Apache Phoenix。作业配置中使用的连接器标识符为 `Jdbc`。
已测试的 Phoenix 版本为 4.x 和 5.x。

底层通过 Phoenix 的 JDBC 驱动执行 `upsert` 语句将每一行写入 HBase。

使用 Java JDBC 连接 Phoenix 有两种方式：

- 通过 **thick** 驱动连接 ZooKeeper 集群；
- 通过 **thin** 驱动连接 Phoenix Query Server。

> **提示 1：** 默认使用（thin）驱动 jar。如果需要使用（thick）驱动或者其他版本的 Phoenix（thin）
> 驱动，需要重新编译 `connector-jdbc` 模块。
>
> **提示 2：** 当前接收器不支持精确一次语义，因为 Phoenix 暂不支持 XA 事务。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称           | 类型   | 是否必填 | 默认值 | 描述                                                                                            |
|----------------|--------|----------|--------|-------------------------------------------------------------------------------------------------|
| driver         | String | 是       | -      | JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用 `org.apache.phoenix.queryserver.client.Driver`。 |
| url            | String | 是       | -      | JDBC 连接 URL。thick 驱动使用 `jdbc:phoenix:localhost:2182/hbase`，thin 驱动使用 `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。 |
| query          | String | 是       | -      | 写入数据时执行的 Phoenix upsert 语句，例如 `upsert into test.sink(age, name) values(?, ?)`。`?` 占位符会按位置绑定到上游行字段。 |
| common-options |        | 否       | -      | 接收器插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。            |

### driver [string]

JDBC 驱动类。thick 驱动使用 `org.apache.phoenix.jdbc.PhoenixDriver`，thin 驱动使用
`org.apache.phoenix.queryserver.client.Driver`。

### url [string]

JDBC 连接 URL。thick 驱动使用 `jdbc:phoenix:localhost:2182/hbase`，thin 驱动使用
`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`。

### query [string]

写入数据时执行的 Phoenix upsert 语句。`?` 占位符会按位置绑定到上游行字段，因此 upsert 中列的
顺序需要和上游 `schema.fields` 的字段顺序一致。表名要使用带 schema 的完全限定名（例如
`test.sink`），不能只写表名。

### common options

接收器插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。

## 任务示例

### 使用 thick 驱动

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        age = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [10, "jared"] }
      { kind = INSERT, fields = [20, "huan"] }
    ]
  }
}

sink {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "upsert into test.sink(age, name) values(?, ?)"
  }
}
```

### 使用 thin 驱动

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        age = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [10, "jared"] }
      { kind = INSERT, fields = [20, "huan"] }
    ]
  }
}

sink {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "upsert into test.sink(age, name) values(?, ?)"
  }
}
```

## 变更日志

<ChangeLog />