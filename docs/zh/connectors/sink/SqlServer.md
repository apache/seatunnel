import ChangeLog from '../changelog/connector-jdbc.md';

# SQLServer

> JDBC SQLServer Sink 连接器

## 支持的 SQL Server 版本

- server:2008（或更高版本，仅供参考）

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 将数据写入 SQL Server。本连接器继承了 [Jdbc 接收器连接器](./Jdbc.md) 的全部选项，并使用 Microsoft SQL Server JDBC 驱动。

支持批处理和流处理模式、并发写入以及精确一次语义（使用 XA 事务保证）。在配置了
`primary_keys` 和 `generate_sink_sql` 时，也支持接收上游的 CDC 变更事件。

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

> 使用 `XA 事务` 来保证 `精确一次`。因此仅支持支持 `XA 事务` 的数据库。可以通过设置 `is_exactly_once=true` 和 `max_retries=0` 来启用。

## 支持的数据源信息

| 数据源     | 支持的版本               | 驱动类名                                      | URL 格式                            | Maven 依赖                                                                  |
|------------|--------------------------|-----------------------------------------------|-------------------------------------|-----------------------------------------------------------------------------|
| SQL Server | 支持版本 >= 2008         | com.microsoft.sqlserver.jdbc.SQLServerDriver  | jdbc:sqlserver://localhost:1433     | [下载](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## 数据库依赖

> 请下载支持列表中对应的 'Maven' 依赖，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录中。<br/>
> 例如 SQL Server 数据源：`cp mssql-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

| SQL Server 数据类型                                              | SeaTunnel 数据类型                                                                              |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| BIT                                                              | BOOLEAN                                                                                          |
| TINYINT<br/>SMALLINT                                             | SHORT                                                                                            |
| INTEGER                                                          | INT                                                                                              |
| BIGINT                                                           | LONG                                                                                             |
| DECIMAL<br />NUMERIC<br />MONEY<br />SMALLMONEY                  | DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数)))                              |
| REAL                                                             | FLOAT                                                                                            |
| FLOAT                                                            | DOUBLE                                                                                           |
| CHAR<br />NCHAR<br />VARCHAR<br />NTEXT<br />NVARCHAR<br />TEXT | STRING                                                                                           |
| DATE                                                             | LOCAL_DATE                                                                                       |
| TIME                                                             | LOCAL_TIME                                                                                       |
| DATETIME<br />DATETIME2<br />SMALLDATETIME<br />DATETIMEOFFSET   | LOCAL_DATE_TIME                                                                                  |
| TIMESTAMP<br />BINARY<br />VARBINARY<br />IMAGE<br />UNKNOWN     | 尚未支持                                                                                         |

## 接收器选项

本连接器使用的选项与 [Jdbc 接收器连接器](./Jdbc.md) 完全一致。下表列出了 SQL Server 相关选项中
与通用 JDBC 选项有差异的部分；其他通用选项请参考链接页面中的权威描述。

| 名称                          | 类型    | 是否必填 | 默认值                       | 描述                                                                                                                                                                                              |
|-------------------------------|---------|----------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | 是       | -                            | JDBC 连接的 URL。参考示例：`jdbc:sqlserver://localhost:1433;databaseName=mydatabase`，其中 `databaseName` 指定连接的默认数据库。                                                                  |
| driver                        | String  | 是       | -                            | 用于连接远程数据源的 JDBC 类名，SQL Server 使用 `com.microsoft.sqlserver.jdbc.SQLServerDriver`。                                                                                                |
| username                      | String  | 是       | -                            | 连接实例的用户名。同时接受 `user` 作为 `username` 的别名。                                                                                                                                          |
| password                      | String  | 是       | -                            | 连接实例的密码。                                                                                                                                                                                  |
| query                         | String  | 否       | -                            | 使用此 SQL 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 优先级更高。                                                                                                                       |
| database                      | String  | 否       | -                            | 使用此 `database` 和 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                  |
| table                         | String  | 否       | -                            | 使用 `database` 和此 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                  |
| primary_keys                  | Array   | 否       | -                            | 此选项用于在自动生成 SQL 时支持 `insert`、`delete` 和 `update` 等操作。                                                                                                                            |
| connection_check_timeout_sec  | Int     | 否       | 30                           | 用于验证连接完成的数据库操作的等待时间（秒）。                                                                                                                                                     |
| max_retries                   | Int     | 否       | 0                            | 提交失败（executeBatch）的重试次数。                                                                                                                                                               |
| batch_size                    | Int     | 否       | 1000                         | 对于批量写入，当缓冲记录数达到 `batch_size` 时，数据会刷新到数据库。如果 `batch_interval_ms` 大于 0，经过指定时间也会触发刷新。                                                                       |
| batch_interval_ms             | Long    | 否       | 0                            | 写入触发的定时刷新间隔，单位毫秒。`0` 表示关闭定时刷新；大于 0 时，写入器会在每条记录写入时检查间隔，达到间隔后同步刷新。                                                                            |
| is_exactly_once               | Boolean | 否       | false                        | 是否启用精确一次语义，将使用 Xa 事务。如果启用，需要设置 `xa_data_source_class_name`。                                                                                                              |
| generate_sink_sql             | Boolean | 否       | false                        | 根据要写入的数据库表生成 SQL 语句。                                                                                                                                                                |
| xa_data_source_class_name     | String  | 否       | -                            | 数据库驱动的 XA 数据源类名，例如 SQL Server 为 `com.microsoft.sqlserver.jdbc.SQLServerXADataSource`，其他数据源请参考 [Jdbc 选项附录](./Jdbc.md#sink-options)。                                   |
| max_commit_attempts           | Int     | 否       | 3                            | 事务提交失败的重试次数。                                                                                                                                                                           |
| transaction_timeout_sec       | Int     | 否       | -1                           | 事务打开后的超时时间，默认为 -1（永不超时）。注意：设置超时可能会影响精确一次语义。                                                                                                                  |
| auto_commit                   | Boolean | 否       | true                         | 默认启用自动事务提交。                                                                                                                                                                             |
| properties                    | Map     | 否       | -                            | 额外的 JDBC 连接参数。当 `properties` 和 URL 中存在相同参数时，优先级由 SQL Server JDBC 驱动决定。                                                                                                 |
| common-options                |         | 否       | -                            | 接收器插件通用参数，详情请参考 [Sink Common Options](../common-options/sink-common-options.md)。                                                                                                    |
| schema_save_mode              | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST  | 同步任务启动前，控制目标表结构的处理方式。                                                                                                                                                          |
| data_save_mode                | Enum    | 否       | APPEND_DATA                  | 同步任务启动前，控制目标表已有数据的处理方式。                                                                                                                                                      |
| custom_sql                    | String  | 否       | -                            | 当 `data_save_mode` 为 `CUSTOM_PROCESSING` 时，填写同步任务启动前需要执行的 SQL。                                                                                                                  |
| enable_upsert                 | Boolean | 否       | true                         | 通过主键存在启用 upsert。如果任务中没有键重复数据，将此参数设置为 `false` 可以加快数据导入速度。                                                                                                   |
| multi_table_sink_replica      | Int     | 否       | 1                            | 多表写入时使用的 Sink Writer 副本数量。                                                                                                                                                            |

### Schema Save Mode（结构保存模式）

`schema_save_mode` 控制任务启动前对目标表结构的处理方式：

- `CREATE_SCHEMA_WHEN_NOT_EXIST`（默认）：目标表不存在时自动创建；存在则跳过。
- `RECREATE_SCHEMA`：目标表存在时先删除，再用上游结构重建。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：目标表不存在时立即报错。
- `IGNORE`：不做任何处理，由下游自行管理。

### Data Save Mode（数据保存模式）

`data_save_mode` 控制任务启动前对目标表已有数据的处理方式：

- `APPEND_DATA`（默认）：保留已有数据，向表中追加新行。
- `DROP_DATA`：保留表结构，删除已有数据。
- `CUSTOM_PROCESSING`：执行用户通过 `custom_sql` 提供的预处理 SQL。
- `ERROR_WHEN_DATA_EXISTS`：目标表已有数据时立即报错。

## 任务示例

### 简单示例

> 从 SQL Server 读取数据并直接写入另一张表。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    query = "select id, name, age from column_type_test.dbo.full_types_jdbc"
  }
}

transform {
}

sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    query = "insert into full_types_jdbc_sink(id, name, age) values(?, ?, ?)"
  }
}
```

### CDC（变更数据捕获）事件

> 我们也支持 CDC 变更数据。在这种情况下，需要配置 `database`、`table` 和 `primary_keys`。

```hocon
sink {
  Jdbc {
    plugin_input = "customers_cdc"
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    generate_sink_sql = true
    database = "column_type_test"
    table = "dbo.full_types_sink"
    batch_size = 100
    primary_keys = ["id"]
  }
}
```

### 精确一次接收器

> 事务性写入可能较慢，但数据更准确。

```hocon
sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    max_retries = 0
    query = "insert into full_types_jdbc_sink(id, name, age) values(?, ?, ?)"
    is_exactly_once = true
    xa_data_source_class_name = "com.microsoft.sqlserver.jdbc.SQLServerXADataSource"
  }
}
```

### Save Mode 示例

> 每次运行时重建目标表，并清空已有数据后再写入。

```hocon
sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "SA"
    password = "Y.sa123456"
    generate_sink_sql = true
    database = "column_type_test"
    table = "dbo.full_types_sink"
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "DROP_DATA"
  }
}
```

### 多表写入

> 在 URL 中使用 `${database_name}` 和 `${table_name}` 占位符，把不同上游表的数据路由到对应的目标表。

```hocon
sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=${database_name}"
    username = "SA"
    password = "Y.sa123456"
    generate_sink_sql = true
    database = "${database_name}"
    table = "${table_name}"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

## 变更日志

<ChangeLog />