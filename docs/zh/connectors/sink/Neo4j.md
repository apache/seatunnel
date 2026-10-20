import ChangeLog from '../changelog/connector-neo4j.md';

# Neo4j

> Neo4j Sink 连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

Neo4j Sink 连接器通过执行 Cypher 语句把 SeaTunnel 数据写入 Neo4j。它支持逐条写入，
也支持使用 Cypher `UNWIND` 批量写入。

`neo4j-java-driver` 版本：4.4.9

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 名称                         | 类型      | 是否必填 | 默认值        | 描述                                                                                                           |
|----------------------------|---------|------|------------|--------------------------------------------------------------------------------------------------------------|
| uri                        | String  | 是    | -          | Neo4j 连接地址，例如 `neo4j://localhost:7687` 或 `bolt://localhost:7687`。                                           |
| username                   | String  | 否    | -          | Neo4j 用户名，需要和 `password` 一起使用。`username`、`bearer_token`、`kerberos_ticket` 三种认证方式至少配置一种。          |
| password                   | String  | 否    | -          | Neo4j 密码。配置 `username` 时必须配置。                                                                            |
| bearer_token               | String  | 否    | -          | 用于 Neo4j 认证的 bearer token。                                                                                |
| kerberos_ticket            | String  | 否    | -          | 用于 Neo4j 认证的 Kerberos ticket。                                                                             |
| database                   | String  | 是    | -          | Neo4j 数据库名。                                                                                                |
| query                      | String  | 是    | -          | 写入数据使用的 Cypher 语句。`ONE_BY_ONE` 模式使用 `$name` 这类占位符；`BATCH` 模式使用 `UNWIND $batch AS row`。             |
| queryParamPosition         | Object  | 仅 ONE_BY_ONE | -          | Cypher 参数名和输入行字段位置的映射。`write_mode = "ONE_BY_ONE"` 时必须填写。                                               |
| max_batch_size             | Integer | 否    | 500        | `write_mode = "BATCH"` 时，单个事务最多写入的数据条数，必须大于 0。                                                         |
| write_mode                 | String  | 否    | ONE_BY_ONE | 写入模式。可选值为 `ONE_BY_ONE` 和 `BATCH`。                                                                         |
| max_transaction_retry_time | Long    | 否    | 30         | 最大事务重试时间，单位为秒。                                                                                            |
| max_connection_timeout     | Long    | 否    | 30         | 建立 TCP 连接的最大等待时间，单位为秒。                                                                                   |
| common-options             | config  | 否    | -          | Sink 通用选项，详见 [Sink 通用选项](../common-options/sink-common-options.md)。                                       |

## 注意事项

- 至少配置一种认证方式：用户名密码、bearer token 或 Kerberos ticket。如果同时配置多种方式，优先级依次为用户名密码、bearer token、Kerberos ticket。
- `ONE_BY_ONE` 模式下，`queryParamPosition` 用来把 Cypher 占位符映射到输入行的字段位置。
- `BATCH` 模式下，查询语句应使用 `UNWIND $batch AS row`，连接器会通过 `batch` 变量传入一批数据。
- `queryParamPosition` 中的字段位置从 `0` 开始，顺序对应上游输入表结构。
- `BATCH` 模式下，每个 `row` 使用上游字段名取值，因此 Cypher 语句里的字段名需要和上游表结构一致。

## 逐条写入示例

```bash
sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    max_transaction_retry_time = 10
    max_connection_timeout = 10

    query = "CREATE (a:Person {name: $name, age: $age})"
    queryParamPosition = {
      name = 0
      age = 1
    }
  }
}
```

## 批量写入示例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    row.num = 1000
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    write_mode = "BATCH"
    max_batch_size = 500
    max_transaction_retry_time = 3
    max_connection_timeout = 10

    query = "UNWIND $batch AS row CREATE (n:BatchLabel) SET n.name = row.name, n.age = row.age"
  }
}
```

## 变更日志

<ChangeLog />
