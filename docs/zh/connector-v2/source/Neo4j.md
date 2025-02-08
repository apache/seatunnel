# Neo4j

> Neo4j 源连接器器

## 描述

从 `Neo4j` 读取数据

`neo4j-java-driver` 版本 4.4.9

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义拆分](../../concept/connector-v2-features.md)

## 配置选项

| 名称                         | 类型     | 是否必须 | 默认值 |
|----------------------------|--------|------|-----|
| uri                        | String | 是    | -   |
| username                   | String | 否    | -   |
| password                   | String | 否   | -   |
| bearer_token               | String | 否   | -   |
| kerberos_ticket            | String | 否   | -   |
| database                   | String | 是    | -   |
| query                      | String | 是    | -   |
| schema                     | Object | 是    | -   |
| max_transaction_retry_time | Long   | 否   | 30  |
| max_connection_timeout     | Long   | 否   | 30  |

### uri [string]

`Neo4j`数据库的URI，参考配置： `neo4j://localhost:7687`。

### username [string]

`Neo4j`用户名。

### password [string]

`Neo4j`密码。如果提供了“用户名”，则需要。

### bearer_token [string]

`Neo4j`的`base64`编码`bearer token`用于鉴权。

### kerberos_ticket [string]

`Neo4j`的`base64`编码`kerberos ticket`用于鉴权。

### database [string]

数据库名。

### query [string]

查询语句。

### schema.fields [string]

返回`query` 的字段。

查看 [列投影](../../concept/connector-v2-features.md)

### max_transaction_retry_time [long]

最大事务重试时间（秒）。如果超过，则事务失败。

### max_connection_timeout [long]

等待TCP连接建立的最长时间（秒）。

## 示例

```
source {
    Neo4j {
        uri = "neo4j://localhost:7687"
        username = "neo4j"
        password = "1234"
        database = "neo4j"
        max_transaction_retry_time = 1
        max_connection_timeout = 1
        query = "MATCH (a:Person) RETURN a.name, a.age"
        schema {
            fields {
                a.age=INT
                a.name=STRING
            }
        }
    }
}
```



