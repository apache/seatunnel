# SQL Server

> JDBC SQL Server Sink Connector

## Support SQL Server Version

- server:2008 (Or later version for information only)

## Support Those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Supported DataSource Info

| Datasource |   Supported Versions    |                    Driver                    |               Url               |                                       Maven                                       |
|------------|-------------------------|----------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------|
| SQL Server | support version >= 2008 | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example SQL Server datasource: cp mssql-jdbc-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

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

|                   Name                    |  Type   | Required | Default |                                                                                                                 Description                                                                                                                  |
|-------------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: jdbc:sqlserver://localhost:1433;databaseName=mydatabase                                                                                                                                     |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source,<br/> if you use sqlServer the value is `com.microsoft.sqlserver.jdbc.SQLServerDriver`.                                                                                        |
| user                                      | String  | No       | -       | Connection instance user name                                                                                                                                                                                                                |
| password                                  | String  | No       | -       | Connection instance password                                                                                                                                                                                                                 |
| query                                     | String  | No       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                       |
| database                                  | String  | No       | -       | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                     |
| table                                     | String  | No       | -       | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                         |
| primary_keys                              | Array   | No       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                          |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false   | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance |
| connection_check_timeout_sec              | Int     | No       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                          |
| max_retries                               | Int     | No       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | No       | 1000    | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                         |
| is_exactly_once                           | Boolean | No       | false   | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                            |
| generate_sink_sql                         | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | No       | -       | The xa data source class name of the database Driver, for example, SqlServer is `com.microsoft.sqlserver.jdbc.SQLServerXADataSource`, and<br/>please refer to appendix for other data sources                                                |
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                          |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                           |
| common-options                            |         | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                  |
| enable_upsert                             | Boolean | No       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                       |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

> This is one that reads Sqlserver data and inserts it directly into another table

```
env {
  # You can set engine configuration here
  parallelism = 10
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "select * from column_type_test.dbo.full_types_jdbc"
    # Parallel sharding reads fields
    partition_column = "id"
    # Number of fragments
    partition_num = 10

  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source/Jdbc
}

transform {

  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "insert into full_types_jdbc_sink( id, val_char, val_varchar, val_text, val_nchar, val_nvarchar, val_ntext, val_decimal, val_numeric, val_float, val_real, val_smallmoney, val_money, val_bit, val_tinyint, val_smallint, val_int, val_bigint, val_date, val_time, val_datetime2, val_datetime, val_smalldatetime ) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"

  }  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc
}
```

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
Jdbc {
  source_table_name = "customers"
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

### Exactly Once Sink

> Transactional writes may be slower but more accurate to the data

```
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "insert into full_types_jdbc_sink( id, val_char, val_varchar, val_text, val_nchar, val_nvarchar, val_ntext, val_decimal, val_numeric, val_float, val_real, val_smallmoney, val_money, val_bit, val_tinyint, val_smallint, val_int, val_bigint, val_date, val_time, val_datetime2, val_datetime, val_smalldatetime ) values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"
    is_exactly_once = "true"

    xa_data_source_class_name = "com.microsoft.sqlserver.jdbc.SQLServerXADataSource"

  }  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc

```

