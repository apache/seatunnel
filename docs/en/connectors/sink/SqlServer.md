import ChangeLog from '../changelog/connector-jdbc.md';

# SQL Server

> JDBC SQL Server Sink Connector

## Support SQL Server Version

- server:2008 (Or later version for information only)

## Support Those engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to SQL Server through JDBC. This connector inherits all options from the
[Jdbc sink connector](./Jdbc.md) and uses the Microsoft JDBC driver for SQL Server.

It supports Batch mode and Streaming mode, concurrent writing, and exactly-once semantics
(using XA transactions). CDC events from upstream are also supported when configured with
`primary_keys` and `generate_sink_sql`.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` and `max_retries=0` to enable it.

## Supported DataSource Info

| Datasource |   Supported Versions    |                    Driver                    |               Url               |                                       Maven                                       |
|------------|-------------------------|----------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------|
| SQL Server | support version >= 2008 | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example SQL Server datasource: cp mssql-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                       SQLserver Data Type                       |                                                                    SeaTunnel Data Type                                                                    |
|-----------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT                                                             | BOOLEAN                                                                                                                                                   |
| TINYINT<br/>SMALLINT                                            | SHORT                                                                                                                                                     |
| INTEGER                                                         | INT                                                                                                                                                       |
| BIGINT                                                          | LONG                                                                                                                                                      |
| DECIMAL<br />NUMERIC<br />MONEY<br />SMALLMONEY                 | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the <br />decimal point.))) |
| REAL                                                            | FLOAT                                                                                                                                                     |
| FLOAT                                                           | DOUBLE                                                                                                                                                    |
| CHAR<br />NCHAR<br />VARCHAR<br />NTEXT<br />NVARCHAR<br />TEXT | STRING                                                                                                                                                    |
| DATE                                                            | LOCAL_DATE                                                                                                                                                |
| TIME                                                            | LOCAL_TIME                                                                                                                                                |
| DATETIME<br />DATETIME2<br />SMALLDATETIME<br />DATETIMEOFFSET  | LOCAL_DATE_TIME                                                                                                                                           |
| TIMESTAMP<br />BINARY<br />VARBINARY<br />IMAGE<br />UNKNOWN    | Not supported yet                                                                                                                                         |

## Sink Options

This connector uses the same set of options as the [Jdbc sink connector](./Jdbc.md). The options
listed below cover everything specific to SQL Server; for options that are identical to the generic
JDBC sink, see the linked page for the canonical description.

|                   Name                    |  Type   | Required | Default |                                                                                                                 Description                                                                                                                  |
|-------------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:sqlserver://localhost:1433;databaseName=mydatabase`. The `databaseName` parameter selects the default database for the connection.                                                    |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source. For SQL Server use `com.microsoft.sqlserver.jdbc.SQLServerDriver`.                                                                                                            |
| username                                  | String  | Yes      | -       | Connection instance user name. `user` is also accepted as a fallback alias.                                                                                                                                                                  |
| password                                  | String  | Yes      | -       | Connection instance password.                                                                                                                                                                                                                 |
| query                                     | String  | No       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                       |
| database                                  | String  | No       | -       | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                     |
| table                                     | String  | No       | -       | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                         |
| primary_keys                              | Array   | No       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                          |
| connection_check_timeout_sec              | Int     | No       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                          |
| max_retries                               | Int     | No       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | No       | 1000    | For batch writing, when the number of buffered records reaches `batch_size`, the data will be flushed into the database. If `batch_interval_ms` is greater than 0, elapsed time can also trigger a flush.                                    |
| batch_interval_ms                         | Long    | No       | 0       | Write-triggered flush interval in milliseconds. `0` disables time-based flushing. When greater than 0, the writer checks elapsed time on each record and flushes synchronously when the interval is reached.                                  |
| is_exactly_once                           | Boolean | No       | false   | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to set `xa_data_source_class_name`.                                                                                                                 |
| generate_sink_sql                         | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | No       | -       | The xa data source class name of the database Driver, for example, SqlServer is `com.microsoft.sqlserver.jdbc.SQLServerXADataSource`, and please refer to [Jdbc options appendix](./Jdbc.md#sink-options) for other data sources.            |
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect exactly-once semantics                                                                                              |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                           |
| properties                                | Map     | No       | -       | Additional JDBC connection parameters. When the same parameter exists in both `properties` and the URL, priority depends on the SQL Server JDBC driver.                                                                                        |
| common-options                            |         | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                                                                                     |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronization task starts, controls how the target table schema is handled.                                                                                                                                                       |
| data_save_mode                            | Enum    | No       | APPEND_DATA | Before the synchronization task starts, controls how existing target table data is handled.                                                                                                                                                    |
| custom_sql                                | String  | No       | -       | When `data_save_mode` is `CUSTOM_PROCESSING`, fill this option with SQL that can be executed before synchronization starts.                                                                                                                   |
| enable_upsert                             | Boolean | No       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                       |
| multi_table_sink_replica                  | Int     | No       | 1       | The number of sink writer replicas used when writing multiple tables.                                                                                                                                                                         |

### Schema Save Mode

`schema_save_mode` controls what happens to the target schema before the job starts:

- `CREATE_SCHEMA_WHEN_NOT_EXIST` (default): create the target table when it does not exist; skip if it already exists.
- `RECREATE_SCHEMA`: drop the target table if it exists and recreate it from the upstream schema.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: fail fast when the target table does not exist.
- `IGNORE`: do nothing and let downstream handle the schema.

### Data Save Mode

`data_save_mode` controls how existing data in the target table is handled when the job starts:

- `APPEND_DATA` (default): append new rows to the existing table.
- `DROP_DATA`: keep the table and delete the existing data.
- `CUSTOM_PROCESSING`: run the user-supplied `custom_sql` once before the job starts.
- `ERROR_WHEN_DATA_EXISTS`: fail fast when the target table already has rows.

## Task Example

### Simple

> Read data from SQL Server and write it directly to another SQL Server table.

```hocon
env {
  # You can set engine configuration here
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

### CDC (Change Data Capture) Event

> CDC change data is also supported. In this case you need to configure `database`, `table` and `primary_keys`.

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

### Exactly Once Sink

> Transactional writes may be slower but more accurate to the data.

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

### Save Mode Example

> Recreate the target table on each run, drop the existing rows, then load fresh data.

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

### Multiple Table Sink

> Use `${database_name}` and `${table_name}` placeholders in the connection URL or write path so rows
> from different upstream tables are routed to different target tables in a single pipeline.

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

## Changelog

<ChangeLog />