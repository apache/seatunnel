# Neo4j

> Neo4j 写连接器

## 描述

写数据到 `Neo4j`。

`neo4j-java-driver` version 4.4.9

## 主要功能

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 配置选项

| 名称                         | 类型      | 是否必须 | 默认值      |
|----------------------------|---------|------|----------|
| uri                        | String  | 是    | -        |
| username                   | String  | 否    | -        |
| password                   | String  | 否   | -        |
| max_batch_size             | Integer | 否   | -        |
| write_mode                 | String  | 否   | OneByOne |
| bearer_token               | String  | 否   | -        |
| kerberos_ticket            | String  | 否   | -        |
| database                   | String  | 是    | -        |
| query                      | String  | 是    | -        |
| queryParamPosition         | Object  | 是    | -        |
| max_transaction_retry_time | Long    | 否   | 30       |
| max_connection_timeout     | Long    | 否   | 30       |
| common-options             | config  | 否   | -        |

### uri [string]

`Neo4j`数据库的URI，参考配置： `neo4j://localhost:7687`。

### username [string]

`Neo4j`用户名。

### password [string]

`Neo4j`密码。如果提供了“用户名”，则需要。

### max_batch_size[Integer]

`max_batch_size` 是指写入数据时，单个事务中可以写入的最大数据条目数。

### write_mode

默认值为 `oneByOne` ，如果您想批量写入，请将其设置为`Batch`

```cypher
unwind $ttt as row create (n:Label) set n.name = row.name,n.age = rw.age
```

`ttt`代表一批数据。，`ttt`可以是任意字符串，只要它与配置的`batch_data_variable` 匹配。

### bearer_token [string]

`Neo4j`的`base64`编码`bearer token`用于鉴权。

### kerberos_ticket [string]

`Neo4j`的`base64`编码`kerberos ticket`用于鉴权。

### database [string]

数据库名称。

### query [string]

查询语句。包含在运行时用相应值替换的参数占位符。

### queryParamPosition [object]

查询参数的位置映射信息。

键名是参数占位符名称。

关联值是字段在输入数据行中的位置。

### max_transaction_retry_time [long]

最大事务重试时间（秒）。如果超过，则交易失败。

### max_connection_timeout [long]

等待TCP连接建立的最长时间（秒）。

### common options

Sink插件常用参数， 详细信息请参考 [Sink公共配置](../sink-common-options.md)

## OneByOne模式写示例

```
sink {
  Neo4j {
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "1234"
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

## Batch模式写示例
> cypher提供的`unwind`关键字支持批量写入，
> 批量数据的默认变量是batch。如果你写一个批处理写语句， 
> 那么你应该声明 cypher `unwind $batch` 作为行
```
sink {
  Neo4j {
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "neo4j"
    database = "neo4j"
    max_batch_size = 1000
    write_mode = "BATCH"
    max_transaction_retry_time = 3
    max_connection_timeout = 10
    query = "unwind $batch as row  create(n:MyLabel) set n.name = row.name,n.age = row.age"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- 添加 Neo4j 写连接器

### issue ##4835

- 写连接器支持批量写入

