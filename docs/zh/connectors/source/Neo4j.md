import ChangeLog from '../changelog/connector-neo4j.md';

# Neo4j

> Neo4j 源连接器

## 描述

Neo4j 源连接器通过执行 Cypher 查询从 Neo4j 读取数据，并把查询返回字段映射成 SeaTunnel
表结构。

`neo4j-java-driver` 版本：4.4.9

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义切分](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| Neo4j 值类型   | SeaTunnel 数据类型 |
|---------------|--------------------|
| String        | STRING             |
| Boolean       | BOOLEAN            |
| Integer       | INT / BIGINT       |
| Float         | FLOAT / DOUBLE     |
| ByteArray     | BYTES              |
| Date          | DATE               |
| LocalTime     | TIME               |
| LocalDateTime | TIMESTAMP          |
| List          | ARRAY              |
| Map           | MAP                |
| Null          | NULL               |

## 源选项

| 名称                         | 类型     | 是否必填 | 默认值 | 描述                                                                                                  |
|----------------------------|--------|------|-----|-----------------------------------------------------------------------------------------------------|
| uri                        | String | 是    | -   | Neo4j 连接地址，例如 `neo4j://localhost:7687` 或 `bolt://localhost:7687`。                                  |
| username                   | String | 否    | -   | Neo4j 用户名，需要和 `password` 一起使用。`username`、`bearer_token`、`kerberos_ticket` 三种认证方式至少配置一种。 |
| password                   | String | 否    | -   | Neo4j 密码。配置 `username` 时必须配置。                                                                   |
| bearer_token               | String | 否    | -   | 用于 Neo4j 认证的 bearer token。                                                                       |
| kerberos_ticket            | String | 否    | -   | 用于 Neo4j 认证的 Kerberos ticket。                                                                    |
| database                   | String | 是    | -   | Neo4j 数据库名。                                                                                       |
| query                      | String | 是    | -   | 读取数据使用的 Cypher 查询语句。查询返回字段必须和 `schema.fields` 对应。                                           |
| schema                     | Object | 是    | -   | 查询结果对应的 SeaTunnel 表结构，在 `schema.fields` 中配置字段名和类型。                                           |
| max_transaction_retry_time | Long   | 否    | 30  | 最大事务重试时间，单位为秒。                                                                                   |
| max_connection_timeout     | Long   | 否    | 30  | 建立 TCP 连接的最大等待时间，单位为秒。                                                                          |

## 注意事项

- 认证方式只选一种：用户名密码、bearer token 或 Kerberos ticket。
- `query` 决定返回哪些字段，`schema.fields` 必须写清这些返回字段和对应类型。
- 查询返回字段名可以包含点号，例如从节点属性返回的 `t.string`。
- `MAP` 字段的 key 必须是 `STRING`，例如 `MAP<STRING, INT>`。
- Neo4j 的整数和浮点数会按 `schema.fields` 中声明的 SeaTunnel 类型转换；如果数值可能超过 `INT` 或 `FLOAT` 范围，建议使用 `BIGINT` 或 `DOUBLE`。

## 任务示例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    max_transaction_retry_time = 1
    max_connection_timeout = 1

    query = "MATCH (t:Test) WITH *, t{.int} AS _map RETURN t.string, t.boolean, t.long, t.double, t.byteArray, t.date, t.localDateTime, _map, t.list, t.int, t.float"

    schema {
      fields {
        t.string = STRING
        t.boolean = BOOLEAN
        t.long = BIGINT
        t.double = DOUBLE
        t.null = NULL
        t.byteArray = BYTES
        t.date = DATE
        t.localDateTime = TIMESTAMP
        _map = "MAP<STRING, INT>"
        t.list = "ARRAY<INT>"
        t.int = INT
        t.float = FLOAT
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
