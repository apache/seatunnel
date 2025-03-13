# SQLServer

> JDBC SQLServer Sink 连接器

## 支持的 SQL Server 版本

- server:2008（或更高版本，仅供参考）

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 写入数据。支持批处理和流处理模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> 使用 `Xa 事务` 来保证 `精确一次`。因此仅支持支持 `Xa 事务` 的数据库。可以通过设置 `is_exactly_once=true` 来启用。

## 支持的数据源信息

| 数据源    | 支持的版本               | 驱动类名                                      | URL 格式                                   | Maven 依赖                                                                                   |
|-----------|--------------------------|-----------------------------------------------|--------------------------------------------|---------------------------------------------------------------------------------------------|
| SQL Server | 支持版本 >= 2008        | com.microsoft.sqlserver.jdbc.SQLServerDriver  | jdbc:sqlserver://localhost:1433            | [下载](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc)               |

## 数据库依赖

> 请下载支持列表中对应的 'Maven' 依赖，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录中。<br/>
> 例如 SQL Server 数据源：`cp mssql-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

| SQL Server 数据类型                     | SeaTunnel 数据类型                                                                                   |
|-----------------------------------------|------------------------------------------------------------------------------------------------------|
| BIT                                     | BOOLEAN                                                                                             |
| TINYINT<br/>SMALLINT                    | SHORT                                                                                               |
| INTEGER                                 | INT                                                                                                 |
| BIGINT                                  | LONG                                                                                                |
| DECIMAL<br />NUMERIC<br />MONEY<br />SMALLMONEY | DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数)))                                |
| REAL                                    | FLOAT                                                                                               |
| FLOAT                                   | DOUBLE                                                                                              |
| CHAR<br />NCHAR<br />VARCHAR<br />NTEXT<br />NVARCHAR<br />TEXT | STRING                                                                                              |
| DATE                                    | LOCAL_DATE                                                                                          |
| TIME                                    | LOCAL_TIME                                                                                          |
| DATETIME<br />DATETIME2<br />SMALLDATETIME<br />DATETIMEOFFSET | LOCAL_DATE_TIME                                                                                     |
| TIMESTAMP<br />BINARY<br />VARBINARY<br />IMAGE<br />UNKNOWN | 尚未支持                                                                                            |

## 接收器选项

| 名称                              | 类型    | 是否必填 | 默认值  | 描述                                                                                                                                                                                                 |
|-----------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                               | String  | 是       | -       | JDBC 连接的 URL。参考示例：`jdbc:sqlserver://localhost:1433;databaseName=mydatabase`                                                                                                                |
| driver                            | String  | 是       | -       | 用于连接远程数据源的 JDBC 类名，如果使用 SQL Server，值为 `com.microsoft.sqlserver.jdbc.SQLServerDriver`。                                                                                           |
| user                              | String  | 否       | -       | 连接实例的用户名                                                                                                                                                                                     |
| password                          | String  | 否       | -       | 连接实例的密码                                                                                                                                                                                       |
| query                             | String  | 否       | -       | 使用此 SQL 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 优先级更高。                                                                                                                         |
| database                          | String  | 否       | -       | 使用此 `database` 和 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                   |
| table                             | String  | 否       | -       | 使用 `database` 和此 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                   |
| primary_keys                      | Array    | 否       | -       | 此选项用于在自动生成 SQL 时支持 `insert`、`delete` 和 `update` 等操作。                                                                                                                              |
| support_upsert_by_query_primary_key_exist | Boolean  | 否       | false   | 选择使用 INSERT SQL、UPDATE SQL 来处理更新事件（INSERT, UPDATE_AFTER），基于查询主键是否存在。此配置仅在数据库不支持 upsert 语法时使用。**注意**：此方法性能较低。                                   |
| connection_check_timeout_sec      | Int    | 否       | 30      | 用于验证连接完成的数据库操作的等待时间（秒）。                                                                                                                                                       |
| max_retries                       | Int    | 否       | 0       | 提交失败（executeBatch）的重试次数。                                                                                                                                                                 |
| batch_size                        | Int    | 否       | 1000    | 对于批量写入，当缓冲的记录数达到 `batch_size` 或时间达到 `checkpoint.interval` 时，数据将被刷新到数据库中。                                                                                           |
| is_exactly_once                   | Boolean  | 否       | false   | 是否启用精确一次语义，将使用 Xa 事务。如果启用，需要设置 `xa_data_source_class_name`。                                                                                                               |
| generate_sink_sql                 | Boolean  | 否       | false   | 根据要写入的数据库表生成 SQL 语句。                                                                                                                                                                  |
| xa_data_source_class_name         | String  | 否       | -       | 数据库驱动的 XA 数据源类名，例如 SQL Server 为 `com.microsoft.sqlserver.jdbc.SQLServerXADataSource`，其他数据源请参考附录。                                                                          |
| max_commit_attempts               | Int    | 否       | 3       | 事务提交失败的重试次数。                                                                                                                                                                             |
| transaction_timeout_sec           | Int    | 否       | -1      | 事务打开后的超时时间，默认为 -1（永不超时）。注意：设置超时可能会影响精确一次语义。                                                                                                                  |
| auto_commit                       | Boolean  | 否       | true    | 默认启用自动事务提交。                                                                                                                                                                               |
| common-options                    |         | 否       | -       | 接收器插件通用参数，详情请参考 [Sink Common Options](../sink-common-options.md)。                                                                                                                    |
| enable_upsert                     | Boolean  | 否       | true    | 通过主键存在启用 upsert。如果任务中没有键重复数据，将此参数设置为 `false` 可以加快数据导入速度。                                                                                                     |

## 提示

> 如果未设置 `partition_column`，将以单并发运行；如果设置了 `partition_column`，将根据任务的并发度并行执行。

## 任务示例

### 简单示例：

> 这是一个读取 SQL Server 数据并直接插入到另一个表的示例

```
env {
  # 可以在此设置引擎配置
  parallelism = 10
}

source {
  # 这是一个示例源插件，**仅用于测试和演示功能**
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "select * from column_type_test.dbo.full_types_jdbc"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10
  }
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的源插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/source/Jdbc
}

transform {
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的转换插件列表，
  # 请访问 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "insert into full_types_jdbc_sink( id, val_char, val_varchar, val_text, val_nchar, val_nvarchar, val_ntext, val_decimal, val_numeric, val_float, val_real, val_smallmoney, val_money, val_bit, val_tinyint, val_smallint, val_int, val_bigint, val_date, val_time, val_datetime2, val_datetime, val_smalldatetime ) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
  }
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的接收器插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc
}
```

### CDC（变更数据捕获）事件

> 我们也支持 CDC 变更数据。在这种情况下，需要配置 `database`、`table` 和 `primary_keys`。

```
Jdbc {
  plugin_input = "customers"
  driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
  url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  user = SA
  password = "Y.sa123456"
  generate_sink_sql = true
  database = "column_type_test"
  table = "dbo.full_types_sink"
  batch_size = 100
  primary_keys = ["id"]
}
```

### 精确一次接收器

> 事务性写入可能较慢，但数据更准确

```
Jdbc {
  driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
  url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  user = SA
  password = "Y.sa123456"
  query = "insert into full_types_jdbc_sink( id, val_char, val_varchar, val_text, val_nchar, val_nvarchar, val_ntext, val_decimal, val_numeric, val_float, val_real, val_smallmoney, val_money, val_bit, val_tinyint, val_smallint, val_int, val_bigint, val_date, val_time, val_datetime2, val_datetime, val_smalldatetime ) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
  is_exactly_once = "true"
  xa_data_source_class_name = "com.microsoft.sqlserver.jdbc.SQLServerXADataSource"
}

# 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的接收器插件列表，
# 请访问 https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc
```
