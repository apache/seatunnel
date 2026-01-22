import ChangeLog from '../changelog/connector-jdbc.md';

# DuckDB

> JDBC DuckDB Sink 连接器

## 支持 DuckDB 版本

- 0.8.x/0.9.x/0.10.x/1.x

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 jdbc 写入数据。支持批处理模式和流处理模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [CDC](../../concept/connector-v2-features.md)

> 使用 `Xa 事务` 来确保 `精确一次`。因此只支持支持 `Xa 事务` 的数据库的 `精确一次`。您可以设置 `is_exactly_once=true` 来启用它。

## 支持的数据源信息

| 数据源    | 支持的版本              | 驱动器                     | 网址                               | Maven下载链接                                                       |
|--------|--------------------|-------------------------|----------------------------------|-----------------------------------------------------------------|
| DuckDB | 不同的依赖版本具有不同的驱动程序类。 | org.duckdb.DuckDBDriver | jdbc:duckdb:/path/to/database.db | [下载](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) |

## 数据类型映射

| SeaTunnel 数据类型                  | DuckDB 数据类型    |
|---------------------------------|----------------|
| BOOLEAN                         | BOOLEAN        |
| TINYINT<br/>SMALLINT<br/>INT    | INTEGER        |
| BIGINT                          | BIGINT         |
| DECIMAL(x,y)(获取指定列的指定列大小.<38)   | DECIMAL(x,y)   |
| DECIMAL(x,y)(获取指定列的指定列大小.>38)   | DECIMAL(38,18) |
| FLOAT                           | FLOAT          |
| DOUBLE                          | DOUBLE         |
| STRING                          | VARCHAR        |
| DATE                            | DATE           |
| TIME                            | TIME           |
| TIMESTAMP                       | TIMESTAMP      |
| BYTES<br/>ARRAY<br/>ROW<br/>MAP | BLOB           |

## Sink 选项

| 名称                           | 类型      | 是否必需 | 默认值                          | 描述                                                                                          |
|------------------------------|---------|------|------------------------------|---------------------------------------------------------------------------------------------|
| url                          | String  | 是    | -                            | JDBC 连接的 URL。参考案例：jdbc:duckdb:/path/to/database.db                                          |
| driver                       | String  | 是    | -                            | 用于连接到远程数据源的 jdbc 类名，<br/> 如果您使用 DuckDB，值为 `org.duckdb.DuckDBDriver`。                        |
| username                     | String  | 否    | -                            | 连接实例用户名                                                                                     |
| password                     | String  | 否    | -                            | 连接实例密码                                                                                      |
| query                        | String  | 否    | -                            | 使用此 sql 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 具有更高的优先级                                       |
| database                     | String  | 否    | main                         | 使用此 `database` 和 `table-name` 自动生成 sql 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥且具有更高的优先级。        |
| table                        | String  | 否    | -                            | 使用数据库和此表名自动生成 sql 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥且具有更高的优先级。                             |
| primary_keys                 | Array   | 否    | -                            | 此选项用于在自动生成 sql 时支持 `insert`、`delete` 和 `update` 等操作。                                        |
| connection_check_timeout_sec | Int     | 否    | 30                           | 等待用于验证连接的数据库操作完成的时间（以秒为单位）。                                                                 |
| max_retries                  | Int     | 否    | 0                            | 提交失败（executeBatch）的重试次数                                                                     |
| batch_size                   | Int     | 否    | 1000                         | 对于批量写入，当缓冲记录数达到 `batch_size` 数量或时间达到 `checkpoint.interval`<br/>时，数据将被刷新到数据库中                |
| is_exactly_once              | Boolean | 否    | false                        | 是否启用精确一次语义，将使用 Xa 事务。如果开启，您需要<br/>设置 `xa_data_source_class_name`。                           |
| generate_sink_sql            | Boolean | 否    | false                        | 根据您要写入的数据库表生成 sql 语句                                                                        |
| xa_data_source_class_name    | String  | 否    | -                            | 数据库驱动程序的 xa 数据源类名，例如，DuckDB 是 `org.duckdb.DuckDBXADataSource`，<br/>其他数据源请参考附录               |
| max_commit_attempts          | Int     | 否    | 3                            | 事务提交失败的重试次数                                                                                 |
| transaction_timeout_sec      | Int     | 否    | -1                           | 事务打开后的超时时间，默认为 -1（永不超时）。请注意，设置超时可能会影响<br/>精确一次语义                                            |
| auto_commit                  | Boolean | 否    | true                         | 默认启用自动事务提交                                                                                  |
| field_ide                    | String  | 否    | -                            | 标识从源同步到接收器时字段是否需要转换。`ORIGINAL` 表示不需要转换；`UPPERCASE` 表示转换为大写；`LOWERCASE` 表示转换为小写。             |
| properties                   | Map     | 否    | -                            | 附加连接配置参数，当 properties 和 URL 具有相同参数时，优先级由 <br/>驱动程序的具体实现确定。例如，在 DuckDB 中，properties 优先于 URL。 |
| common-options               |         | 否    | -                            | Sink 插件通用参数，详情请参考 [Sink Common Options](../sink-common-options.md)                          |
| schema_save_mode             | Enum    | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | 在同步任务开启之前，针对目标端已有的表结构选择不同的处理方案。                                                             |
| data_save_mode               | Enum    | 否    | APPEND_DATA                  | 在同步任务开启之前，针对目标端已有数据选择不同的处理方案。                                                               |
| custom_sql                   | String  | 否    | -                            | 当 data_save_mode 选择 CUSTOM_PROCESSING 时，应填写 CUSTOM_SQL 参数。此参数通常填写可执行的 SQL。SQL 将在同步任务之前执行。   |
| enable_upsert                | Boolean | 否    | true                         | 通过 primary_keys 存在启用 upsert，如果任务只有 `insert`，将此参数设置为 `false` 可以加快数据导入速度                      |

### 提示

> 如果未设置 partition_column，它将以单一并发运行，如果设置了 partition_column，它将根据任务的并发度并行执行。

## 任务示例


### 简单

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    row_num = 1000
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = "duckdb"
    password = ""
  }
}
```

### CDC（变更数据捕获）事件

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/test"
    username = "root"
    password = "123456"
    table-names = ["test.user"]
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = "duckdb"
    password = ""
    generate_sink_sql = true
    # 您需要同时配置 database 和 table
    database = main
    table = "sink_table"
    primary_keys = ["id"]
  }
}
```

### 精确一次

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    row_num = 1000
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = ""
    password = ""

    is_exactly_once = "true"

    xa_data_source_class_name = "org.duckdb.DuckDBXADataSource"
  }
}
```

## Changelog

<ChangeLog />